package com.dcp.seatunnel.transform;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CdcSeqTransform(设计文档 §2.6/§3.2/§4.4 上游采集端)。
 *
 * <p>每行处理流水线:
 * <pre>
 *   RowKind → INSERT/UPDATE/DELETE
 *   businessKey = business_key_fields 值按声明顺序以 '|' 拼接(空值按 null_key_mode)
 *   seq        = 位点即序号(路径 1, emit_position=true) 或 元库 seq_counter 号段(旧)
 *   bucket     = MurmurHash3(businessKey) % bucketCount(固定桶, 扩容映射热更新在 sink 侧)
 *   输出       = [__bucket, __payload(CdcRecord JSON)]
 * </pre>
 *
 * <p>位点发号三态(⑤ emit_position 灰度开关):
 * <ul>
 *   <li>true(严格位点): 增量行必须有补丁透传位点(缺位点=混发 → fail-fast);
 *       快照行(无位点)统一编码为快照级号 S=f(snapshot_start_position)(④);</li>
 *   <li>false(纯号段): 计数器模式, 忽略补丁位点列(灰度双跑对照旧链路);</li>
 *   <li>缺省(兼容): 有位点用位点, 否则计数器(补丁落地前旧配置不受影响)。</li>
 * </ul>
 *
 * <p>要点:
 * <ul>
 *   <li>CDC 无 before image(2.3.12), 主键修改无法拆解 → 按业务铁律留给消费端告警(文档 §3.5);</li>
 *   <li>UPDATE_BEFORE(若引擎发出)也按 UPDATE 处理: 序号小于 AFTER, 目标侧 seq 裁判天然收敛;</li>
 *   <li>JDBC 连接在 open() 建立(构造期不可序列化), close() 释放;</li>
 *   <li>字段值统一字符串化(目标分表 VARCHAR 承载, 与 AtomicUpsertDao.str 一致)。</li>
 * </ul>
 */
public class CdcSeqTransform implements org.apache.seatunnel.api.transform.SeaTunnelMapTransform<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private static final String KEY_JOIN = "|";

    private final CdcSeqTransformConfig config;

    /** open() 初始化(transient: 不可序列化) */
    private transient Connection seqConn;
    private transient PreparedStatement allocStmt;
    private transient PreparedStatement insertStmt;
    private transient PreparedStatement selectStmt;
    /** 当前已领号段的游标: [nextSeq, rangeEnd] 闭区间, nextSeq 为下一个待用号 */
    private transient long nextSeq;
    private transient long rangeEnd;

    private static final ObjectMapper OM = new ObjectMapper();

    private static final org.slf4j.Logger LOGGER =
            org.slf4j.LoggerFactory.getLogger(CdcSeqTransform.class);

    /** ③ 体系重置检测: 位点模式历史最大 seq(transient: 重启后首行不告警, 观测级守卫) */
    private transient long lastSeq = Long.MIN_VALUE;
    /** ④ 快照级号 S=f(快照起点位点)缓存: 全批同号(重放恒定) */
    private transient long snapshotSeqValue;
    private transient boolean snapshotSeqReady;

    public CdcSeqTransform(CdcSeqTransformConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "CdcSeqTransform";
    }

    @Override
    public void open() {
        // 严格位点模式下计数器不可达(缺位点 fail-fast), 不连元库;
        // 号段/兼容模式仍需 seq_counter
        if (config.isStrictPositionMode()) {
            return;
        }
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.seqConn = DriverManager.getConnection(
                    config.getSeqDbUrl(), config.getSeqDbUser(), config.getSeqDbPassword());
            this.allocStmt = seqConn.prepareStatement(
                    "UPDATE seq_counter SET cur = cur + ? WHERE table_name = ?");
            this.insertStmt = seqConn.prepareStatement(
                    "INSERT INTO seq_counter (table_name, cur) VALUES (?, ?)");
            this.selectStmt = seqConn.prepareStatement(
                    "SELECT cur FROM seq_counter WHERE table_name = ?");
            this.nextSeq = 0;
            this.rangeEnd = 0;
            allocateRange(); // 预热第一个号段(失败尽早暴露)
        } catch (Exception e) {
            throw new IllegalStateException("CdcSeqTransform 连接元库失败: " + e.getMessage(), e);
        }
    }

    /** 领下一个 seq; 号段用尽时再领一段(区间预分配, 崩溃丢号段留间隙无害) */
    private long nextSeq() throws Exception {
        if (nextSeq > rangeEnd) {
            allocateRange();
        }
        return nextSeq++;
    }

    /** 领号段 [start, end]: UPDATE 行锁 +1 段, 首见插入 [1, batch] */
    private void allocateRange() throws Exception {
        int batch = Math.max(1, config.getSeqBatch());
        String table = config.getSeqKey();
        allocStmt.setInt(1, batch);
        allocStmt.setString(2, table);
        int rows = allocStmt.executeUpdate();
        if (rows == 0) {
            insertStmt.setString(1, table);
            insertStmt.setInt(2, batch);
            insertStmt.executeUpdate();
        }
        selectStmt.setString(1, table);
        long end;
        try (ResultSet rs = selectStmt.executeQuery()) {
            rs.next();
            end = rs.getLong(1);
        }
        rangeEnd = end;
        nextSeq = end - batch + 1;
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        try {
            RowKind rk = row.getRowKind();
            String rowKind = rk == RowKind.DELETE ? "DELETE"
                    : (rk == RowKind.INSERT ? "INSERT" : "UPDATE");

            // ① 源字段(全部字符串化, 与目标 VARCHAR 承载一致)
            Map<String, Object> srcFields = new LinkedHashMap<>();
            String[] names = config.getInputNames();
            Object[] values = row.getFields();
            for (int i = 0; i < names.length && i < values.length; i++) {
                Object v = values[i];
                srcFields.put(names[i], v == null ? null : String.valueOf(v));
            }

            // ② seq: 位点即序号(路径 1, connector 补丁在行末透传位点列, 不进 schema)
            //    快照行与补丁未落地(无位点列)回退计数器; 快照批/流批分派独立, 两体系不互比
            String srcPos = values.length > names.length ? strOf(values[names.length]) : null;
            boolean isSnapshot = values.length > names.length + 1
                    && "true".equalsIgnoreCase(strOf(values[names.length + 1]));
            long seq = resolveSeq(srcPos, isSnapshot);

            // ③ 字段映射执行器(向导 Step3: 直连/计算/生成 + 加工链; 输出模型字段)
            FieldMappingExecutor executor = config.getFieldMappingExecutor();
            Map<String, Object> fields = executor != null && executor.isEnabled()
                    ? executor.apply(srcFields, seq) : srcFields;
            // 系统保留列 seq 不进业务字段(由 CdcRecord 顶层携带, 目标表独立列)
            fields.remove("seq");

            // ③.5 行级转换(仅 filter/filter-kind): 不过则丢弃该行(不进 Kafka)
            RowChainExecutor rowChain = config.getRowChainExecutor();
            if (rowChain != null && rowChain.isEnabled() && !rowChain.pass(rowKind, fields)) {
                return null;
            }

            // ④ 业务主键: 模型字段(business_key_fields)从映射后字段取值; 未映射直连取源字段
            StringBuilder key = new StringBuilder();
            for (String f : config.getBusinessKeyFields()) {
                Object v = fields.containsKey(f) ? fields.get(f) : srcFields.get(f);
                if (key.length() > 0) {
                    key.append(KEY_JOIN);
                }
                key.append(v == null ? "" : v);
            }
            String businessKey = RouteUtil.resolveKey(key.toString(),
                    config.getNullKeyMode(), config.getFallbackValue());

            // Mongo DELETE 事件只带 documentKey(_id), 文档已删无业务字段;
            // 若业务键不含 _id 则无法定位目标行 → 跳过该 DELETE(固有限制, 文档标注)
            if ((businessKey == null || businessKey.isBlank()) && "DELETE".equals(rowKind)) {
                return null;
            }

            // ⑤ CdcRecord 封装
            Map<String, Object> rec = new LinkedHashMap<>();
            rec.put("logicalTable", config.getLogicalTable());
            if (config.getTaskId() != null && !config.getTaskId().isBlank()) {
                rec.put("taskId", config.getTaskId());
            }
            if (config.getSourceDb() != null && !config.getSourceDb().isBlank()) {
                rec.put("sourceDb", config.getSourceDb());
            }
            if (config.getSourceTable() != null && !config.getSourceTable().isBlank()) {
                rec.put("sourceTable", config.getSourceTable());
            }
            if (config.getDatasourceId() != null && !config.getDatasourceId().isBlank()) {
                rec.put("datasourceId", config.getDatasourceId());
            }
            rec.put("businessKey", businessKey);
            rec.put("keyColumns", config.getKeyColumns());
            rec.put("rowKind", rowKind);
            rec.put("seq", seq);
            // ⑤ 发号体系标志(position/compat/counter), 灰度双跑对照用
            rec.put("positionMode", config.getEmitPosition() == null
                    ? "compat" : String.valueOf(config.getEmitPosition()));
            // 快照标志透传(消费端据此分派批/流模板; 补丁未落地 false 等价旧行为)
            rec.put("snapshot", isSnapshot);
            rec.put("fields", fields);
            String payload = OM.writeValueAsString(rec);

            // ⑥ 桶路由(固定桶, sink 侧按 __bucket 分区保序)
            int bucket = Math.floorMod(RouteUtil.hash(businessKey), config.getBucketCount());

            return new SeaTunnelRow(new Object[]{bucket, payload});
        } catch (Exception e) {
            throw new IllegalStateException("CdcSeqTransform 处理失败: " + e.getMessage(), e);
        }
    }

    /**
     * ②④⑤⑥ seq 发号分派(位点/快照级号/号段三路径)。
     *
     * <p>④ 快照行优先走快照级号 S: 快照行位点不逐行单调(快照阶段共享同一位点), 不能作序号;
     * 严格位点模式必配 snapshot_start_position(否则 fail-fast);
     * 兼容模式: 配了起点走 S, 否则回退计数器(补丁未透传快照标志的旧链路)。
     *
     * <p>⑤ 增量行三态: true(严格位点, 位点缺失即混发 fail-fast);
     * false(纯号段, 忽略补丁位点列); 缺省(兼容, 有位点用位点)。
     *
     * <p>⑥ 兼容模式位点编码失败(如 mongo ts_ms 缺失等实测位点取值形态异常)时
     * 不 fail-fast, 告警后回退计数器号段(满位源治理回退分支; 严格位点模式仍 fail-fast)。
     * (包可见: 供同包 PositionCheck 自检直连)
     */
    long resolveSeq(String srcPos, boolean isSnapshot) throws Exception {
        if (isSnapshot) {
            String snap = config.getSnapshotStartPosition();
            if (snap != null && !snap.isBlank()) {
                return snapshotSeq(snap);
            }
            if (config.isStrictPositionMode()) {
                throw new IllegalStateException("快照行需配置 snapshot_start_position"
                        + "(④ 快照级号 S=f(起点位点), fail-fast)");
            }
            return nextSeq();
        }
        if (config.isStrictPositionMode()) {
            if (srcPos != null && !srcPos.isBlank()) {
                checkLayout(srcPos);
                return guardReset(PositionSeq.encode(config.getSourceType(), srcPos));
            }
            throw new IllegalStateException("位点模式下增量行位点缺失, "
                    + "疑似位点透传补丁未生效(守卫: 新旧号混发检测, fail-fast)");
        }
        if (config.getEmitPosition() != null && !config.getEmitPosition()) {
            return nextSeq(); // 纯号段模式(灰度双跑对照旧链路)
        }
        // 兼容模式(缺省): 有位点用位点, 位点不可用回退计数器(⑥ 满位源治理回退分支)
        if (srcPos != null && !srcPos.isBlank()) {
            try {
                checkLayout(srcPos);
                return guardReset(PositionSeq.encode(config.getSourceType(), srcPos));
            } catch (IllegalStateException e) {
                LOGGER.warn("兼容模式位点编码失败, 回退计数器号段: {} ({})", srcPos, e.getMessage());
            }
        }
        return nextSeq();
    }

    /**
     * ② 逐源键名映射表校验 + 位点类型与配置源类型一致校验
     * (守卫: 未知格式/错位体系 fail-fast; 旧配置无 source_type 时跳过)
     */
    private void checkLayout(String srcPos) {
        String st = config.getSourceType();
        if (st == null || st.isBlank()) {
            return;
        }
        int cut = srcPos.indexOf(':');
        String posType = cut > 0 ? srcPos.substring(0, cut) : srcPos;
        if (!st.equalsIgnoreCase(posType)) {
            throw new IllegalStateException("位点类型与配置源类型不符: 位点=" + posType
                    + " 配置=" + st + "(守卫: 新旧号混发检测, fail-fast)");
        }
        SourcePositionSpec.checkLayout(st, srcPos);
    }

    /** ④ 快照级号 S=f(快照起点位点): 同批共享同号, 缓存编码结果(重放恒定) */
    private long snapshotSeq(String snap) {
        if (!snapshotSeqReady) {
            checkLayout(snap);
            snapshotSeqValue = PositionSeq.encode(config.getSourceType(), snap);
            snapshotSeqReady = true;
        }
        return snapshotSeqValue;
    }

    /** ③ 体系重置检测: 新 seq 比历史峰值回退超阈值 → 告警(不丢数据, 人工裁决) */
    private long guardReset(long seq) {
        if (lastSeq != Long.MIN_VALUE && seq < lastSeq - config.getSeqResetThreshold()) {
            LOGGER.error("seq 体系回退: 新 seq={} 远小于历史峰值 {} (阈值 {}), "
                            + "疑似源端位点体系重置(重建/GTID 重置), 下游只认更大 seq 的幂等裁判将拒绝新数据, 请人工裁决",
                    seq, lastSeq, config.getSeqResetThreshold());
        }
        if (seq > lastSeq) {
            lastSeq = seq;
        }
        return seq;
    }

    private static String strOf(Object v) {
        return v == null ? null : String.valueOf(v);
    }

    private int indexOf(String field) {
        String[] names = config.getInputNames();
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(field)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return List.of(config.getOutputTable());
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return config.getOutputTable();
    }

    @Override
    public void close() {
        try {
            if (allocStmt != null) allocStmt.close();
            if (insertStmt != null) insertStmt.close();
            if (selectStmt != null) selectStmt.close();
            if (seqConn != null) seqConn.close();
        } catch (Exception e) {
            // 忽略关闭异常
        }
    }
}

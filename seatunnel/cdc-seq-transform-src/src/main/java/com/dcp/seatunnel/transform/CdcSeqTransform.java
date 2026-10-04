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
 *   seq        = 位点即序号(严格位点一态)
 *   bucket     = MurmurHash3(businessKey) % bucketCount(固定桶, 扩容映射热更新在 sink 侧)
 *   输出       = [__bucket, __payload(CdcRecord JSON)]
 * </pre>
 *
 * <p>位点发号(新方案: 仅严格位点一态, 三态/号段已废弃):
 * <ul>
 *   <li>增量行必须有补丁透传位点(缺位点 = 混发 → fail-fast);</li>
 *   <li>快照行(位点主段空形态, 实测定案)统一编码为快照级号 S=f(snapshot_start_position)(方案 A 后端下发, 必配);</li>
 * </ul>
 *
 * <p>主键修改检测(新方案): 补丁透传 before 主键值(行尾第三列 __src_before_key),
 * beforeKey ≠ businessKey → 写 sync_alert 告警 + 抛异常停止任务(checkpoint 保留,
 * watchdog 查 OPEN 告警不自动 restore, 人工处理后重建/续采)。
 *
 * <p>要点:
 * <ul>
 *   <li>CDC 无 before image(2.3.12), 主键修改无法自动拆解 → 按拍板策略暂停告警人工介入;</li>
 *   <li>缺陷 7(2026-10-05 实测): UPDATE_BEFORE 旧值行与 UPDATE 新值行位点相同 → seq 相同,
 *       消费端同号裁判会拒新值行(变更丢失)且写旧值行破坏全程正确性 → UPDATE_BEFORE 行直接丢弃
 *       (before 主键检测由补丁透传 __src_before 列承担, 该行无信息量);</li>
 *   <li>严格位点模式不连 seq_counter(位点即序号); 告警落库 sync_alert 为独立职责(可选配置);</li>
 *   <li>字段值统一字符串化(目标分表 VARCHAR 承载, 与 AtomicUpsertDao.str 一致)。</li>
 * </ul>
 */
public class CdcSeqTransform implements org.apache.seatunnel.api.transform.SeaTunnelMapTransform<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private static final String KEY_JOIN = "|";

    private final CdcSeqTransformConfig config;

    /** open() 初始化(transient: 不可序列化): 告警落库连接(可选, 主键修改/缺列告警写 sync_alert) */
    private transient Connection alertConn;
    private transient PreparedStatement alertStmt;

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
        // 严格位点一态: 不连 seq_counter(位点即序号, 无号可领);
        // 告警落库为独立职责: 配置了告警库则建连接(主键修改告警写 sync_alert), 未配置仅抛异常
        try {
            if (config.getAlertDbUrl() != null && !config.getAlertDbUrl().isBlank()) {
                Class.forName("com.mysql.cj.jdbc.Driver");
                this.alertConn = DriverManager.getConnection(
                        config.getAlertDbUrl(), config.getAlertDbUser(), config.getAlertDbPassword());
                this.alertStmt = alertConn.prepareStatement(
                        "INSERT INTO sync_alert (logical_table, task_id, alert_type, detail, status) "
                                + "VALUES (?, ?, ?, ?, 'OPEN')");
            }
        } catch (Exception e) {
            throw new IllegalStateException("CdcSeqTransform 告警库连接失败: " + e.getMessage(), e);
        }
    }

    /** 主键修改告警: 写 sync_alert(失败仅告警日志, 不阻断异常抛出) */
    private void alertPrimaryKeyChange(String beforeKey, String businessKey) {
        alertGeneric("primary_key_change", beforeKey + " → " + businessKey);
    }

    /** 通用告警落库: 写 sync_alert(失败仅告警日志, 不阻断异常抛出) */
    private void alertGeneric(String alertType, String detail) {
        try {
            if (alertStmt != null) {
                alertStmt.setString(1, config.getLogicalTable());
                alertStmt.setLong(2, config.getTaskId() == null || config.getTaskId().isBlank()
                        ? 0 : Long.parseLong(config.getTaskId()));
                alertStmt.setString(3, alertType);
                alertStmt.setString(4, detail);
                alertStmt.executeUpdate();
            }
        } catch (Exception e) {
            LOGGER.error("{} 告警落库失败({}): {}", alertType, config.getLogicalTable(), e.getMessage());
        }
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        try {
            RowKind rk = row.getRowKind();
            // 缺陷 7(实测): UPDATE_BEFORE(旧值行)与 UPDATE(新值行)同 source record → 位点透传相同
            // → seq 相同, 消费端写者列裁判同号拒写会导致新值丢失, 且写旧值行破坏全程正确性(中间态回退)。
            // before 主键检测由补丁透传的 __src_before 列承担, UPDATE_BEFORE 行无信息量 → 丢弃(不进 Kafka)
            if (rk == RowKind.UPDATE_BEFORE) {
                return null;
            }
            String rowKind = rk == RowKind.DELETE ? "DELETE"
                    : (rk == RowKind.INSERT ? "INSERT" : "UPDATE");

            // ① 源字段(全部字符串化, 与目标 VARCHAR 承载一致)
            //    补丁透传三列 __src_pos/__snapshot/__src_before 已由 widen 工艺并入
            //    producedType(位于行尾三列, 不进业务字段)
            Map<String, Object> srcFields = new LinkedHashMap<>();
            String[] names = config.getInputNames();
            Object[] values = row.getFields();
            boolean hasPatchCols = names.length >= 3
                    && "__src_pos".equals(names[names.length - 3]);
            int fieldCount = hasPatchCols ? values.length - 3 : values.length;
            for (int i = 0; i < names.length && i < fieldCount; i++) {
                Object v = values[i];
                srcFields.put(names[i], v == null ? null : String.valueOf(v));
            }

            // ② seq: 位点即序号(严格位点一态, 补丁在行尾透传位点/快照标志/before 三列)
            //    快照行统一编码为快照级号 S=f(snapshot_start_position)(方案 A: 后端读源库当前位点下发)
            String srcPos = hasPatchCols ? strOf(values[values.length - 3]) : null;
            String snapshotFlag = hasPatchCols ? strOf(values[values.length - 2]) : null;
            boolean isSnapshot = hasPatchCols && isSnapshotRow(srcPos, snapshotFlag);
            // before 行透传(行尾第三列, JSON): 解析后按 business_key_fields 拼 beforeKey,
            // 主键修改检测用; 补丁未覆盖时无此列 → null 不检测
            String srcBeforeJson = hasPatchCols ? strOf(values[values.length - 1]) : null;
            String srcBeforeKey = beforeKeyOf(srcBeforeJson);

            long seq = resolveSeq(srcPos, isSnapshot);

            // ③ 字段映射执行器(向导 Step3: 直连/计算/生成 + 加工链; 输出模型字段;
            //    必配 fail-fast, 输出 fields = 声明 modelField 集合, 列裁剪白名单)
            FieldMappingExecutor executor = config.getFieldMappingExecutor();
            Map<String, Object> fields = executor.apply(srcFields, seq);

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

            // ④.5 主键修改检测(新方案): before 主键透传, 与当前业务键不同 = 主键值被修改
            //      → 写 sync_alert 告警 + 抛异常停止任务(checkpoint 保留, watchdog 不自动 restore)
            if (srcBeforeKey != null && !srcBeforeKey.isBlank() && !srcBeforeKey.equals(businessKey)) {
                alertPrimaryKeyChange(srcBeforeKey, businessKey);
                throw new IllegalStateException("PRIMARY_KEY_CHANGE 主键值被修改: " + srcBeforeKey
                        + " → " + businessKey + "(任务已暂停, 请处理 sync_alert 告警后重建任务/映射)");
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
            if (srcBeforeKey != null && !srcBeforeKey.isBlank()) {
                rec.put("beforeKey", srcBeforeKey);
            }
            rec.put("keyColumns", config.getKeyColumns());
            rec.put("rowKind", rowKind);
            rec.put("seq", seq);
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
     * ④ 快照行判定(实测定案 2026-10-05):
     * <ul>
     *   <li>__snapshot 列 = true(将来 Debezium 版本若带 snapshot 键);</li>
     *   <li>位点主段为空 → 快照行。实测 MySQL 快照行 sourceOffset
     *       = {transaction_id=null, ts_sec=..., file=, pos=0}(无 snapshot 键, 位点列
     *       透传为 "mysql::0:"); 增量行 file 必非空 → 主段空仅快照阶段出现。</li>
     * </ul>
     */
    private boolean isSnapshotRow(String srcPos, String snapshotFlag) {
        if ("true".equalsIgnoreCase(snapshotFlag)) {
            return true;
        }
        if (srcPos == null || srcPos.isBlank()) {
            return false;
        }
        int cut = srcPos.indexOf(':');
        String body = cut > 0 ? srcPos.substring(cut + 1) : srcPos;
        String first = body.split(":", -1)[0];
        return first == null || first.isBlank();
    }

    /**
     * ④⑤ seq 发号分派(新方案: 仅严格位点一态)。
     *
     * <p>④ 快照行统一走快照级号 S=f(snapshot_start_position): 快照行位点不逐行单调
     * (快照阶段共享同一位点), 不能作序号; 未配置 snapshot_start_position → fail-fast。
     *
     * <p>⑤ 增量行必须有补丁透传位点, 缺位点 = 新旧号混发 → fail-fast。
     */
    long resolveSeq(String srcPos, boolean isSnapshot) throws Exception {
        if (isSnapshot) {
            String snap = config.getSnapshotStartPosition();
            if (snap == null || snap.isBlank()) {
                throw new IllegalStateException("快照行需配置 snapshot_start_position"
                        + "(④ 快照级号 S=f(起点位点), fail-fast)");
            }
            return snapshotSeq(snap);
        }
        if (srcPos == null || srcPos.isBlank()) {
            throw new IllegalStateException("位点模式下增量行位点缺失, "
                    + "疑似位点透传补丁未生效(守卫: 新旧号混发检测, fail-fast)");
        }
        checkLayout(srcPos);
        return guardReset(PositionSeq.encode(config.getSourceType(), srcPos));
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

    /** ④.5 beforeKey 提取: 从 before 行 JSON(补丁透传)按 business_key_fields 拼接主键值;
     *  null(无 before 列/CDC 无 before image)→ null(不检测);
     *  解析失败 → 告警 + fail-fast(无法完成主键修改检测, 不允许静默降级, 缺陷 5) */
    private String beforeKeyOf(String beforeJson) {
        if (beforeJson == null || beforeJson.isBlank()) {
            return null;
        }
        try {
            Map<String, Object> m = OM.readValue(beforeJson, Map.class);
            StringBuilder key = new StringBuilder();
            for (String f : config.getBusinessKeyFields()) {
                Object v = m.get(f);
                if (key.length() > 0) {
                    key.append(KEY_JOIN);
                }
                key.append(v == null ? "" : v);
            }
            return RouteUtil.resolveKey(key.toString(),
                    config.getNullKeyMode(), config.getFallbackValue());
        } catch (Exception e) {
            alertGeneric("before_parse_fail", "before 行 JSON 解析失败: " + e.getMessage());
            throw new IllegalStateException("before 行 JSON 解析失败(fail-fast, 无法完成主键修改检测): "
                    + e.getMessage(), e);
        }
    }

    private static String strOf(Object v) {
        return v == null ? null : String.valueOf(v);
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
            if (alertStmt != null) alertStmt.close();
            if (alertConn != null) alertConn.close();
        } catch (Exception e) {
            // 忽略关闭异常
        }
    }
}

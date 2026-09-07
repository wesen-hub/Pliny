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
 *   seq        = 元库 seq_counter 区间预分配(行锁并发不重, 允许间隙; open() 首取, 用尽再领)
 *   bucket     = MurmurHash3(businessKey) % bucketCount(固定桶, 扩容映射热更新在 sink 侧)
 *   输出       = [__bucket, __payload(CdcRecord JSON)]
 * </pre>
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

    public CdcSeqTransform(CdcSeqTransformConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "CdcSeqTransform";
    }

    @Override
    public void open() {
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

            // ② 全局 seq(区间预分配)
            long seq = nextSeq();

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
            rec.put("fields", fields);
            String payload = OM.writeValueAsString(rec);

            // ⑥ 桶路由(固定桶, sink 侧按 __bucket 分区保序)
            int bucket = Math.floorMod(RouteUtil.hash(businessKey), config.getBucketCount());

            return new SeaTunnelRow(new Object[]{bucket, payload});
        } catch (Exception e) {
            throw new IllegalStateException("CdcSeqTransform 处理失败: " + e.getMessage(), e);
        }
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

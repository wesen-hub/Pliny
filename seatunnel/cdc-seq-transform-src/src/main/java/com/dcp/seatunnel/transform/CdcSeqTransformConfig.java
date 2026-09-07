package com.dcp.seatunnel.transform;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.ArrayList;
import java.util.List;

/**
 * CdcSeqTransform 配置: 解析选项, 构建输出 CatalogTable。
 *
 * <p>输出 schema 固定两列:
 * <pre>
 *   __bucket   INT    桶号(供 Kafka sink partition_key_fields 做同桶同分区)
 *   __payload  STRING CdcRecord JSON(消费端直接解析)
 * </pre>
 */
public class CdcSeqTransformConfig implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final String logicalTable;
    private final String taskId;
    private final String datasourceId;
    private final String sourceDb;
    private final String sourceTable;
    private final List<String> businessKeyFields;
    private final List<String> keyColumns;
    private final int bucketCount;
    private final RouteUtil.NullKeyMode nullKeyMode;
    private final String fallbackValue;
    private final String seqDbUrl;
    private final String seqDbUser;
    private final String seqDbPassword;
    private final int seqBatch;
    private final String[] inputNames;
    private final CatalogTable outputTable;
    /** 字段映射执行器(向导 Step3 配置; null = 直通) */
    private final FieldMappingExecutor fieldMappingExecutor;
    /** 行级转换执行器(向导 Step4: 仅 filter/filter-kind; null = 不过滤) */
    private final RowChainExecutor rowChainExecutor;

    public CdcSeqTransformConfig(List<CatalogTable> inputTables,
                                 org.apache.seatunnel.api.configuration.ReadonlyConfig options) {
        CatalogTable input = inputTables.get(0);
        SeaTunnelRowType inType = input.getSeaTunnelRowType();
        this.inputNames = inType.getFieldNames();
        this.logicalTable = options.get(CdcSeqTransformFactory.LOGICAL_TABLE);
        this.taskId = options.get(CdcSeqTransformFactory.TASK_ID);
        this.datasourceId = options.get(CdcSeqTransformFactory.DATASOURCE_ID);
        this.sourceDb = options.get(CdcSeqTransformFactory.SOURCE_DB);
        this.sourceTable = options.get(CdcSeqTransformFactory.SOURCE_TABLE);
        this.businessKeyFields = options.get(CdcSeqTransformFactory.BUSINESS_KEY_FIELDS);
        this.keyColumns = options.get(CdcSeqTransformFactory.KEY_COLUMNS);
        this.bucketCount = options.get(CdcSeqTransformFactory.BUCKET_COUNT);
        this.nullKeyMode = "FALLBACK".equalsIgnoreCase(options.get(CdcSeqTransformFactory.NULL_KEY_MODE))
                ? RouteUtil.NullKeyMode.FALLBACK : RouteUtil.NullKeyMode.ERROR;
        this.fallbackValue = options.get(CdcSeqTransformFactory.FALLBACK_VALUE);
        this.seqDbUrl = options.get(CdcSeqTransformFactory.SEQ_DB_URL);
        this.seqDbUser = options.get(CdcSeqTransformFactory.SEQ_DB_USER);
        this.seqDbPassword = options.get(CdcSeqTransformFactory.SEQ_DB_PASSWORD);
        this.seqBatch = options.get(CdcSeqTransformFactory.SEQ_BATCH);
        String fm = options.get(CdcSeqTransformFactory.FIELD_MAPPING);
        this.fieldMappingExecutor = fm == null || fm.isBlank() ? null : new FieldMappingExecutor(fm);
        String rc = options.get(CdcSeqTransformFactory.ROW_CHAIN);
        this.rowChainExecutor = rc == null || rc.isBlank() ? null : new RowChainExecutor(rc);

        // 输出 schema: __bucket INT + __payload STRING(替换输入 schema, 消息体只带这两列)
        List<org.apache.seatunnel.api.table.catalog.Column> cols = new ArrayList<>();
        cols.add(PhysicalColumn.of("__bucket", BasicType.INT_TYPE, (Long) null, true, null, null));
        cols.add(PhysicalColumn.of("__payload", BasicType.STRING_TYPE, (Long) null, true, null, null));
        TableSchema schema = TableSchema.builder().columns(cols).build();
        this.outputTable = CatalogTable.of(input.getTableId(), schema, input.getOptions(),
                input.getPartitionKeys(), input.getComment());
    }

    public String getLogicalTable() { return logicalTable; }
    public String getTaskId() { return taskId; }
    public String getSourceDb() { return sourceDb; }
    public String getSourceTable() { return sourceTable; }
    public String getDatasourceId() { return datasourceId; }

    /**
     * seq 计数键 = (任务实例, 逻辑表, 数据源, 源库, 源表): 每条(任务×源表)采集流一条独立序列;
     * 不同任务的同(逻辑,源)也各自独立、永不互比。缺省逐级回退兼容旧配置。超长时 md5。
     */
    public String getSeqKey() {
        if (sourceDb == null || sourceDb.isBlank() || sourceTable == null || sourceTable.isBlank()) {
            return logicalTable;
        }
        String ds = datasourceId == null || datasourceId.isBlank() ? "0" : datasourceId;
        String tk = taskId == null || taskId.isBlank() ? "0" : taskId;
        String key = tk + "|" + logicalTable + "|" + ds + "|" + sourceDb + "." + sourceTable;
        if (key.length() <= 120) {
            return key;
        }
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] h = md.digest(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder("h");
            for (byte b : h) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return key.substring(0, 120);
        }
    }
    public List<String> getBusinessKeyFields() { return businessKeyFields; }
    public List<String> getKeyColumns() { return keyColumns; }
    public int getBucketCount() { return bucketCount; }
    public RouteUtil.NullKeyMode getNullKeyMode() { return nullKeyMode; }
    public String getFallbackValue() { return fallbackValue; }
    public String getSeqDbUrl() { return seqDbUrl; }
    public String getSeqDbUser() { return seqDbUser; }
    public String getSeqDbPassword() { return seqDbPassword; }
    public int getSeqBatch() { return seqBatch; }
    public String[] getInputNames() { return inputNames; }
    public CatalogTable getOutputTable() { return outputTable; }
    public FieldMappingExecutor getFieldMappingExecutor() { return fieldMappingExecutor; }
    public RowChainExecutor getRowChainExecutor() { return rowChainExecutor; }
}

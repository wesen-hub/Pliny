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
    private final String alertDbUrl;
    private final String alertDbUser;
    private final String alertDbPassword;
    private final String[] inputNames;
    private final CatalogTable outputTable;
    /** 字段映射执行器(必配: 空映射在构造器 fail-fast; 列裁剪白名单) */
    private final FieldMappingExecutor fieldMappingExecutor;
    /** 行级转换执行器(向导 Step4: 仅 filter/filter-kind; null = 不过滤) */
    private final RowChainExecutor rowChainExecutor;
    /** 源库类型(mysql/pg/oracle/sqlserver/mongodb/tidb) */
    private final String sourceType;
    /** ④ 快照起点位点(type:k1:v1...): 快照行统一编码为快照级号 S */
    private final String snapshotStartPosition;
    /** ③ seq 体系回退告警阈值(体系重置检测) */
    private final long seqResetThreshold;

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
        // 缺陷 6c: 空/全空白 fail-fast(optionRule required 只保证键存在, 空列表能漏过;
        // 空业务键 → ERROR 模式全行拒, 空唯一键 → 幂等写无定位列, 均需注册级拦截)
        if (isBlankList(this.businessKeyFields)) {
            throw new IllegalArgumentException("business_key_fields 不能为空(空业务键无法路由/幂等定位, fail-fast)");
        }
        if (isBlankList(this.keyColumns)) {
            throw new IllegalArgumentException("key_columns 不能为空(唯一键列空 → 幂等写无法定位目标行, fail-fast)");
        }
        this.bucketCount = options.get(CdcSeqTransformFactory.BUCKET_COUNT);
        this.nullKeyMode = "FALLBACK".equalsIgnoreCase(options.get(CdcSeqTransformFactory.NULL_KEY_MODE))
                ? RouteUtil.NullKeyMode.FALLBACK : RouteUtil.NullKeyMode.ERROR;
        this.fallbackValue = options.get(CdcSeqTransformFactory.FALLBACK_VALUE);
        this.alertDbUrl = options.get(CdcSeqTransformFactory.ALERT_DB_URL);
        this.alertDbUser = options.get(CdcSeqTransformFactory.ALERT_DB_USER);
        this.alertDbPassword = options.get(CdcSeqTransformFactory.ALERT_DB_PASSWORD);
        String fm = options.get(CdcSeqTransformFactory.FIELD_MAPPING);
        // 强校验必配: 空/缺失由 optionRule required + FieldMappingExecutor 构造器 fail-fast 双保险
        this.fieldMappingExecutor = new FieldMappingExecutor(fm);
        String rc = options.get(CdcSeqTransformFactory.ROW_CHAIN);
        this.rowChainExecutor = rc == null || rc.isBlank() ? null : new RowChainExecutor(rc);
        this.sourceType = options.get(CdcSeqTransformFactory.SOURCE_TYPE);
        this.snapshotStartPosition = options.get(CdcSeqTransformFactory.SNAPSHOT_START_POSITION);
        this.seqResetThreshold = options.get(CdcSeqTransformFactory.SEQ_RESET_THRESHOLD);

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

    public List<String> getBusinessKeyFields() { return businessKeyFields; }
    public List<String> getKeyColumns() { return keyColumns; }
    public int getBucketCount() { return bucketCount; }
    public RouteUtil.NullKeyMode getNullKeyMode() { return nullKeyMode; }
    public String getFallbackValue() { return fallbackValue; }
    public String getAlertDbUrl() { return alertDbUrl; }
    public String getAlertDbUser() { return alertDbUser; }
    public String getAlertDbPassword() { return alertDbPassword; }
    public String[] getInputNames() { return inputNames; }
    public CatalogTable getOutputTable() { return outputTable; }
    public FieldMappingExecutor getFieldMappingExecutor() { return fieldMappingExecutor; }
    public RowChainExecutor getRowChainExecutor() { return rowChainExecutor; }
    public String getSourceType() { return sourceType; }
    public String getSnapshotStartPosition() { return snapshotStartPosition; }
    public long getSeqResetThreshold() { return seqResetThreshold; }

    /** 列表为空或全空白(缺陷 6c fail-fast 判定) */
    private static boolean isBlankList(List<String> list) {
        return list == null || list.isEmpty() || list.stream().allMatch(s -> s == null || s.isBlank());
    }
}

package com.dcp.seatunnel.transform;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.ArrayList;
import java.util.List;

/**
 * RouteTransform 配置: 解析 business_key / 分表参数, 构建输出 CatalogTable。
 *
 * <p>输出 = 输入字段 + 追加三列:
 * <ul>
 *   <li>{@code __bucket}       (INT)    Kafka 固定桶号</li>
 *   <li>{@code __table_no}     (INT)    MySQL 物理表号</li>
 *   <li>{@code __target_table} (STRING) 物理表名, 如 t_order_03, 供 sink 直接使用</li>
 * </ul>
 */
public class RouteTransformConfig {

    public static final String[] ROUTE_FIELDS = {"__bucket", "__table_no", "__target_table"};

    private final String businessKeyField;
    private final int bucketCount;
    private final int tableCount;
    private final String tablePrefix;
    private final int tableNameWidth;
    private final RouteUtil.NullKeyMode nullKeyMode;
    private final String fallbackValue;

    private final int businessKeyIndex;
    private final SeaTunnelRowType outputRowType;
    private final CatalogTable outputTable;

    public RouteTransformConfig(List<CatalogTable> inputTables, ReadonlyConfig options) {
        CatalogTable input = inputTables.get(0);
        SeaTunnelRowType inType = input.getSeaTunnelRowType();

        this.businessKeyField = options.get(RouteTransformFactory.BUSINESS_KEY);
        this.bucketCount = options.get(RouteTransformFactory.BUCKET_COUNT);
        this.tableCount = options.get(RouteTransformFactory.TABLE_COUNT);
        this.tablePrefix = options.get(RouteTransformFactory.TABLE_PREFIX);
        int width = options.get(RouteTransformFactory.TABLE_NAME_WIDTH);
        this.tableNameWidth = width > 0 ? width
                : Math.max(2, String.valueOf(tableCount - 1).length());
        this.nullKeyMode = "FALLBACK".equalsIgnoreCase(
                options.get(RouteTransformFactory.NULL_KEY_MODE))
                ? RouteUtil.NullKeyMode.FALLBACK : RouteUtil.NullKeyMode.ERROR;
        this.fallbackValue = options.get(RouteTransformFactory.FALLBACK_VALUE);

        // 业务主键必须在输入字段里(由上游 AviatorTransform 先做字段归一)
        this.businessKeyIndex = inType.indexOf(businessKeyField);
        if (businessKeyIndex < 0) {
            throw new IllegalArgumentException(
                    "business_key 字段 [" + businessKeyField + "] 不在输入 schema 中: "
                            + String.join(",", inType.getFieldNames()));
        }

        // 输出 schema = 输入字段 + 路由三列
        String[] inNames = inType.getFieldNames();
        SeaTunnelDataType<?>[] inTypes = inType.getFieldTypes();
        String[] outNames = new String[inNames.length + ROUTE_FIELDS.length];
        SeaTunnelDataType<?>[] outTypes = new SeaTunnelDataType<?>[inNames.length + ROUTE_FIELDS.length];
        System.arraycopy(inNames, 0, outNames, 0, inNames.length);
        System.arraycopy(inTypes, 0, outTypes, 0, inTypes.length);
        outNames[inNames.length] = ROUTE_FIELDS[0];
        outNames[inNames.length + 1] = ROUTE_FIELDS[1];
        outNames[inNames.length + 2] = ROUTE_FIELDS[2];
        outTypes[inNames.length] = BasicType.INT_TYPE;
        outTypes[inNames.length + 1] = BasicType.INT_TYPE;
        outTypes[inNames.length + 2] = BasicType.STRING_TYPE;
        this.outputRowType = new SeaTunnelRowType(outNames, outTypes);

        List<org.apache.seatunnel.api.table.catalog.Column> cols = new ArrayList<>();
        for (int i = 0; i < outNames.length; i++) {
            cols.add(PhysicalColumn.of(outNames[i], outTypes[i], (Long) null, true, null, null));
        }
        this.outputTable = CatalogTable.of(input.getTableId(),
                TableSchema.builder().columns(cols).build(),
                input.getOptions(), input.getPartitionKeys(), input.getComment());
    }

    public String getBusinessKeyField() {
        return businessKeyField;
    }

    public int getBusinessKeyIndex() {
        return businessKeyIndex;
    }

    public int getBucketCount() {
        return bucketCount;
    }

    public int getTableCount() {
        return tableCount;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public int getTableNameWidth() {
        return tableNameWidth;
    }

    public RouteUtil.NullKeyMode getNullKeyMode() {
        return nullKeyMode;
    }

    public String getFallbackValue() {
        return fallbackValue;
    }

    public SeaTunnelRowType getOutputRowType() {
        return outputRowType;
    }

    public CatalogTable buildOutputCatalogTable() {
        return outputTable;
    }
}

package com.dcp.seatunnel.transform;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;

/**
 * RouteTransform 工厂（SeaTunnel 2.3.12 SPI, plugin_name = RouteTransform）
 *
 * <p>配置示例:
 * <pre>
 * transform {
 *   RouteTransform {
 *     business_key    = order_id      # 输入中业务主键字段(多表汇聚时是归一后的统一字段)
 *     bucket_count    = 4096          # Kafka 固定桶数(与 topic 映射一致, 定死不可改)
 *     table_count     = 50            # 目标 MySQL 分表数(与建表一致, 定死不可改)
 *     table_prefix    = t_order       # 物理表名前缀 → t_order_03
 *     null_key_mode   = ERROR         # ERROR | FALLBACK
 *     fallback_value  = __NULL_KEY__  # FALLBACK 模式下的兜底键
 *   }
 * }
 * </pre>
 */
public class RouteTransformFactory implements TableTransformFactory {

    public static final Option<String> BUSINESS_KEY = Options.key("business_key")
            .stringType().noDefaultValue().withDescription("业务主键字段名(输入 schema 中)");
    public static final Option<Integer> BUCKET_COUNT = Options.key("bucket_count")
            .intType().defaultValue(4096).withDescription("Kafka 固定桶数");
    public static final Option<Integer> TABLE_COUNT = Options.key("table_count")
            .intType().defaultValue(50).withDescription("目标物理分表数");
    public static final Option<String> TABLE_PREFIX = Options.key("table_prefix")
            .stringType().noDefaultValue().withDescription("物理表名前缀, 如 t_order");
    public static final Option<Integer> TABLE_NAME_WIDTH = Options.key("table_name_width")
            .intType().defaultValue(0).withDescription("表号补零宽度, 0=按表数自动");
    public static final Option<String> NULL_KEY_MODE = Options.key("null_key_mode")
            .stringType().defaultValue("ERROR").withDescription("空主键策略: ERROR | FALLBACK");
    public static final Option<String> FALLBACK_VALUE = Options.key("fallback_value")
            .stringType().defaultValue("__NULL_KEY__").withDescription("FALLBACK 模式兜底键");

    @Override
    public String factoryIdentifier() {
        return "RouteTransform";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BUSINESS_KEY, TABLE_PREFIX)
                .optional(BUCKET_COUNT, TABLE_COUNT, TABLE_NAME_WIDTH, NULL_KEY_MODE, FALLBACK_VALUE)
                .build();
    }

    @Override
    public TableTransform<SeaTunnelRow> createTransform(TableTransformFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        List<CatalogTable> inputTables = context.getCatalogTables();
        RouteTransformConfig config = new RouteTransformConfig(inputTables, options);
        return () -> new RouteTransform(config);
    }
}

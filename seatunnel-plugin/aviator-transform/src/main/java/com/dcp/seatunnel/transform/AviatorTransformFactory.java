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

/** AviatorTransform 工厂（SeaTunnel 2.3.12 SPI, plugin_name = AviatorTransform） */
public class AviatorTransformFactory implements TableTransformFactory {

    public static final Option<List<Object>> MAPPINGS = Options.key("mappings")
            .listType(Object.class).noDefaultValue().withDescription("字段映射列表");
    public static final Option<Object> DICT_DATA = Options.key("dict_data")
            .objectType(Object.class).noDefaultValue().withDescription("字典数据");
    public static final Option<Boolean> FAIL_ON_ERROR = Options.key("fail_on_error")
            .booleanType().defaultValue(false).withDescription("表达式失败是否整任务失败");

    @Override
    public String factoryIdentifier() {
        return "AviatorTransform";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(MAPPINGS, DICT_DATA, FAIL_ON_ERROR)
                .build();
    }

    @Override
    public TableTransform<SeaTunnelRow> createTransform(TableTransformFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        List<CatalogTable> inputTables = context.getCatalogTables();
        AviatorTransformConfig config = new AviatorTransformConfig(inputTables, options);
        return () -> new AviatorTransform(config);
    }
}

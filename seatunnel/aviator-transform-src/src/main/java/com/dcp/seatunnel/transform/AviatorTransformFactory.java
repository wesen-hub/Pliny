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

    /** 嵌套结构（List/Map）在 Zeta LogicalVertex Java 序列化时不可序列化（typesafe Config）, 故用 JSON 字符串承载 */
    public static final Option<String> MAPPING_RULES = Options.key("mapping_rules")
            .stringType().noDefaultValue().withDescription("映射规则 JSON 字符串: {\"mappings\":[{sourceField,targetField,transformType,transformRule,sortNo}]}");
    public static final Option<String> DICT_DATA = Options.key("dict_data")
            .stringType().noDefaultValue().withDescription("字典数据 JSON 字符串: {\"data\":{字典名:{源值:目标值}}}");
    public static final Option<Boolean> FAIL_ON_ERROR = Options.key("fail_on_error")
            .booleanType().defaultValue(false).withDescription("表达式失败是否整任务失败");

    @Override
    public String factoryIdentifier() {
        return "AviatorTransform";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(MAPPING_RULES, DICT_DATA, FAIL_ON_ERROR)
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

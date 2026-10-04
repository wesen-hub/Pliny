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
 * CdcSeqTransform 工厂(SeaTunnel 2.3.12 SPI, plugin_name = CdcSeqTransform)。
 *
 * <p>采集转换器: 置于 CDC source 与 Kafka sink 之间, 每行做四件事:
 * <pre>
 *   ① 列裁剪(白名单): 输出 fields = 声明的 modelField 集合(映射非空强校验)
 *   ② seq = 位点确定性编码(严格位点一态, 缺位点 fail-fast; 快照行 = 快照级号 S)
 *   ③ 桶路由: bucket = MurmurHash3(businessKey) % bucketCount
 *   ④ 封装 CdcRecord JSON(逻辑表/身份/业务主键/唯一键列/rowKind/seq/beforeKey/字段)
 * </pre>
 * 输出两列: {@code __bucket}(INT, 供 Kafka sink partition_key_fields 保序) +
 * {@code __payload}(STRING, 消费端可直接反序列化为 CdcRecord)。
 *
 * <p>配置示例:
 * <pre>
 * transform {
 *   CdcSeqTransform {
 *     logical_table      = t_order
 *     business_key_fields = [order_id]      # 参与业务主键的 CDC 字段(多列组合按声明顺序拼)
 *     key_columns        = [order_id]       # 目标唯一键列
 *     bucket_count       = 4096
 *     null_key_mode      = ERROR            # ERROR | FALLBACK
 *     fallback_value     = __NULL_KEY__
 *     field_mapping      = "[{modelField:..., source:{kind:field-map,srcField:...}}]"  # 强校验必配
 *     snapshot_start_position = "mysql:binlog.000001:4:0"  # 快照级号 S=f(起点位点), 必配
 *     alert_db_url       = "jdbc:mysql://mysql:3306/dcp?..."  # 告警落库(主键修改写 sync_alert, 可选)
 *   }
 * }
 * </pre>
 */
public class CdcSeqTransformFactory implements TableTransformFactory {

    public static final Option<String> TASK_ID = Options.key("task_id")
            .stringType().noDefaultValue().withDescription("任务实例 id(写者列第一维度, 同(逻辑,源)不同任务各自独立)");
    public static final Option<String> LOGICAL_TABLE = Options.key("logical_table")
            .stringType().noDefaultValue().withDescription("逻辑表名(CdcRecord.logicalTable + 告警归属)");
    public static final Option<String> DATASOURCE_ID = Options.key("datasource_id")
            .stringType().noDefaultValue().withDescription("源数据源 id(写者列维度, 防不同数据源同名表串列)");
    public static final Option<String> SOURCE_DB = Options.key("source_db")
            .stringType().noDefaultValue().withDescription("源库名(写者列维度 + CdcRecord.sourceDb)");
    public static final Option<String> SOURCE_TABLE = Options.key("source_table")
            .stringType().noDefaultValue().withDescription("源表名(写者列维度 + CdcRecord.sourceTable)");
    public static final Option<List<String>> BUSINESS_KEY_FIELDS = Options.key("business_key_fields")
            .listType().noDefaultValue().withDescription("参与业务主键的字段(按声明顺序拼接)");
    public static final Option<List<String>> KEY_COLUMNS = Options.key("key_columns")
            .listType().noDefaultValue().withDescription("目标唯一键列(幂等写定位)");
    public static final Option<Integer> BUCKET_COUNT = Options.key("bucket_count")
            .intType().defaultValue(4096).withDescription("Kafka 固定桶数");
    public static final Option<String> NULL_KEY_MODE = Options.key("null_key_mode")
            .stringType().defaultValue("ERROR").withDescription("空主键策略: ERROR | FALLBACK");
    public static final Option<String> FALLBACK_VALUE = Options.key("fallback_value")
            .stringType().defaultValue("__NULL_KEY__").withDescription("FALLBACK 模式兜底键");
    public static final Option<String> ALERT_DB_URL = Options.key("alert_db_url")
            .stringType().noDefaultValue().withDescription("告警库 JDBC URL(sync_alert, 主键修改/before 解析失败告警; 必配, 缺陷 5)");
    public static final Option<String> ALERT_DB_USER = Options.key("alert_db_user")
            .stringType().noDefaultValue().withDescription("告警库用户名(必配)");
    public static final Option<String> ALERT_DB_PASSWORD = Options.key("alert_db_password")
            .stringType().noDefaultValue().withDescription("告警库密码(必配)");
    public static final Option<String> FIELD_MAPPING = Options.key("field_mapping")
            .stringType().noDefaultValue().withDescription("字段映射配置 JSON(强校验必配: [{modelField, source:{kind,srcField,expr}, steps:[...]}], 列裁剪白名单)");
    public static final Option<String> ROW_CHAIN = Options.key("row_chain")
            .stringType().noDefaultValue().withDescription("行级转换链 JSON(向导 Step4: 仅 filter/filter-kind, [{type,enabled,params}])");
    public static final Option<String> SOURCE_TYPE = Options.key("source_type")
            .stringType().noDefaultValue().withDescription("源库类型(mysql/pg/oracle/sqlserver/mongodb/tidb), 位点编码分派");
    public static final Option<String> SNAPSHOT_START_POSITION = Options.key("snapshot_start_position")
            .stringType().noDefaultValue().withDescription("④ 快照起点位点(type:k1:v1...), 快照行统一编码为快照级号 S(必配)");
    public static final Option<Long> SEQ_RESET_THRESHOLD = Options.key("seq_reset_threshold")
            .longType().defaultValue(1_000_000_000_000L).withDescription("③ seq 体系回退告警阈值(体系重置检测)");

    @Override
    public String factoryIdentifier() {
        return "CdcSeqTransform";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(LOGICAL_TABLE, TASK_ID, DATASOURCE_ID, SOURCE_DB, SOURCE_TABLE,
                        BUSINESS_KEY_FIELDS, KEY_COLUMNS, FIELD_MAPPING,
                        ALERT_DB_URL, ALERT_DB_USER, ALERT_DB_PASSWORD)
                .optional(BUCKET_COUNT, NULL_KEY_MODE, FALLBACK_VALUE, ROW_CHAIN, SOURCE_TYPE,
                        SNAPSHOT_START_POSITION, SEQ_RESET_THRESHOLD)
                .build();
    }

    @Override
    public TableTransform<SeaTunnelRow> createTransform(TableTransformFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        List<CatalogTable> inputTables = context.getCatalogTables();
        CdcSeqTransformConfig config = new CdcSeqTransformConfig(inputTables, options);
        return () -> new CdcSeqTransform(config);
    }
}

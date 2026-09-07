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
 * <p>采集端打标器: 置于 CDC source 与 Kafka sink 之间, 每行做三件事:
 * <pre>
 *   ① 领全局 seq(元库区间预分配, 行锁并发不重)并打标
 *   ② 桶路由: bucket = MurmurHash3(businessKey) % bucketCount
 *   ③ 封装 CdcRecord JSON(逻辑表/业务主键/唯一键列/rowKind/seq/字段) → 输出列 __payload
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
 *     seq_db_url         = "jdbc:mysql://mysql:3306/dcp?useSSL=false&allowPublicKeyRetrieval=true"
 *     seq_db_user        = root
 *     seq_db_password    = root123
 *     seq_batch          = 1000             # 领号区间大小(摊销 DB 开销)
 *   }
 * }
 * </pre>
 */
public class CdcSeqTransformFactory implements TableTransformFactory {

    public static final Option<String> TASK_ID = Options.key("task_id")
            .stringType().noDefaultValue().withDescription("任务实例 id(seq 键第一维度, 同(逻辑,源)不同任务各自独立)");
    public static final Option<String> LOGICAL_TABLE = Options.key("logical_table")
            .stringType().noDefaultValue().withDescription("逻辑表名(seq 计数器 + CdcRecord.logicalTable)");
    public static final Option<String> DATASOURCE_ID = Options.key("datasource_id")
            .stringType().noDefaultValue().withDescription("源数据源 id(seq 键维度, 防不同数据源同名表串号)");
    public static final Option<String> SOURCE_DB = Options.key("source_db")
            .stringType().noDefaultValue().withDescription("源库名(seq 键维度 + CdcRecord.sourceDb)");
    public static final Option<String> SOURCE_TABLE = Options.key("source_table")
            .stringType().noDefaultValue().withDescription("源表名(seq 键维度 + CdcRecord.sourceTable)");
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
    public static final Option<String> SEQ_DB_URL = Options.key("seq_db_url")
            .stringType().noDefaultValue().withDescription("元库 JDBC URL(seq_counter)");
    public static final Option<String> SEQ_DB_USER = Options.key("seq_db_user")
            .stringType().noDefaultValue().withDescription("元库用户名");
    public static final Option<String> SEQ_DB_PASSWORD = Options.key("seq_db_password")
            .stringType().noDefaultValue().withDescription("元库密码");
    public static final Option<Integer> SEQ_BATCH = Options.key("seq_batch")
            .intType().defaultValue(1000).withDescription("领号区间大小");
    public static final Option<String> FIELD_MAPPING = Options.key("field_mapping")
            .stringType().noDefaultValue().withDescription("字段映射配置 JSON(向导 Step3: [{modelField, source:{kind,srcField,expr}, steps:[...]}])");
    public static final Option<String> ROW_CHAIN = Options.key("row_chain")
            .stringType().noDefaultValue().withDescription("行级转换链 JSON(向导 Step4: 仅 filter/filter-kind, [{type,enabled,params}])");

    @Override
    public String factoryIdentifier() {
        return "CdcSeqTransform";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(LOGICAL_TABLE, BUSINESS_KEY_FIELDS, KEY_COLUMNS, SEQ_DB_URL, SEQ_DB_USER, SEQ_DB_PASSWORD)
                .optional(BUCKET_COUNT, NULL_KEY_MODE, FALLBACK_VALUE, SEQ_BATCH, FIELD_MAPPING, ROW_CHAIN,
                        DATASOURCE_ID, SOURCE_DB, SOURCE_TABLE, TASK_ID)
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

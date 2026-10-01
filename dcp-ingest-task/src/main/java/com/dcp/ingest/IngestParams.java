package com.dcp.ingest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 底座任务 params(backend 提交任务时组装; 与 tagengine-service v2TaskParams 同源同键纪律)。
 *
 * <pre>
 * logicalTable  逻辑表(消费 dcp.in.{逻辑表} 分区; 信封目标 topic 缺省从元库 logical_table.topic 读)
 * jdbcUrl/user/password  元库连接(seq_counter 领号 / dlt_message 落库 / logical_table·model_definition 读)
 * kafkaBootstrap  信封目标(dcp.{逻辑表})生产者地址
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IngestParams {

    public String logicalTable;
    public String jdbcUrl;
    public String user;
    public String password;
    public String kafkaBootstrap;
    /** 可选: 信封目标 topic 覆盖(缺省 = 元库 logical_table.topic) */
    public String targetTopic;
}

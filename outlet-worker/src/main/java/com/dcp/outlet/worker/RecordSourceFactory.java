package com.dcp.outlet.worker;

/** 消费源工厂:生产实现创建 KafkaConsumer(单分区组 assign);测试实现给脚本化桩。 */
public interface RecordSourceFactory {

    RecordSource create(CreateUnitCmd cmd);
}

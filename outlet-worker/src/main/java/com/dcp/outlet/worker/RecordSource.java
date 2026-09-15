package com.dcp.outlet.worker;

import java.util.List;

/**
 * 消费源端口(生产实现 = KafkaConsumer 单分区组 assign/seek/commitSync;测试实现 = 脚本化桩)。
 *
 * <p>刻意**不把 KafkaConsumer 暴露给上层** —— 便于用内存桩验证语义(空批/异常/重投/停机)。
 */
public interface RecordSource {

    /** 拉取一批消息;超时无数据返回空列表。 */
    List<RawMessage> poll(long timeoutMillis);

    /** 设置消费位置(单元启动时注入起点)。 */
    void seek(int partition, long offset);

    /** 提交位点:Kafka 语义是"下一条要读的位置"。 */
    void commitSync(int partition, long offset);

    /** 唤醒阻塞中的 poll(用于优雅停机)。 */
    void wakeup();

    void close();
}

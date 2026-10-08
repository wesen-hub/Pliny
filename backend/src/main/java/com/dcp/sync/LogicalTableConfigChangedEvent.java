package com.dcp.sync;

/**
 * 逻辑表配置变更事件(审计落库后发布; 采集端/消费端热更新通知载体)。
 *
 * <p>action 取值:
 * <ul>
 *   <li>CREATE —— 逻辑表首次配置(消费端新提交, 采集端不动作)</li>
 *   <li>UPDATE —— 已有配置变更但 topic 未变(消费端重新提交)</li>
 *   <li>TOPIC_CHANGED —— topic 变更(采集端需重提任务换 sink topic)</li>
 *   <li>PARTITIONS —— 分区数/桶映射扩容(消费端重新提交; 采集端写分区由 key 哈希自适应)</li>
 * </ul>
 */
public record LogicalTableConfigChangedEvent(String tableName, String action,
                                             String oldTopic, String newTopic) {
}

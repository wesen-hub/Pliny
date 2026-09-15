package com.dcp.outlet.control;

import java.util.Map;

/**
 * 增量段交**外部引擎**的端口(方案 C:增量段不跑 v2 消费单元,而是交给 Flink 等外部引擎)。
 *
 * <p>控制面算好"每分区起点"后,不再规划/下发 v2 单元,而是把起点一次性播种给外部引擎适配器
 * (由适配器生成 job-json、提交 Flink REST 等)。位点由外部引擎自管,进度仍按 Kafka 消费组读取。
 */
public interface ExternalIncrementalDispatcher {

    /**
     * 把增量段交接给外部引擎。
     *
     * @param task       任务(含 params、logicId、contractRef、epoch)
     * @param topic      消费的 Kafka topic
     * @param stream     流名(逻辑表名)
     * @param startOffsets 每分区 → 起始 offset(第一个 commitTs &gt; T0 的位点)
     */
    void dispatch(TaskRecord task, String topic, String stream, Map<Integer, Long> startOffsets);
}

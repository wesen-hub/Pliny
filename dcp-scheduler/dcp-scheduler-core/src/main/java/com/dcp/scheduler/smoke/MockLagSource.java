package com.dcp.scheduler.smoke;

import com.dcp.scheduler.lag.KafkaLagSource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 冒烟用 mock Kafka:endOffset = MockExecutor.PRODUCED 生产水位。
 * 命名协议:topic = "demo-{taskId}"(SmokeMain 提交时约定),PRODUCED key = taskId/partId。
 * 查不到 topic 时返回空表(调用方对缺分区回退 rps 近似,不阻塞)。
 */
public class MockLagSource implements KafkaLagSource {

    @Override
    public Map<Integer, Long> endOffsets(String topic) {
        String taskId = topic.startsWith("demo-") ? topic.substring("demo-".length()) : topic;
        Map<Integer, Long> out = new HashMap<>();
        for (Map.Entry<String, AtomicLong> e : MockExecutor.PRODUCED.entrySet()) {
            String[] kv = e.getKey().split("/", 2);
            if (kv.length == 2 && taskId.equals(kv[0])) {
                out.put(Integer.parseInt(kv[1]), e.getValue().get());
            }
        }
        return out;
    }
}

package com.dcp.module.outlet.sdk;

import com.dcp.module.outlet.OutletProperties;
import com.dcp.module.outlet.metrics.OutletMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费方 SDK 的**真实运行器**(调试/演示用):把 SDK 从"单测"推到"真跑"。
 *
 * <p>做的事情与一个真实消费方完全一致:
 * <ol>
 *   <li>`open(startPolicy)`:有已存位点就续跑;否则按策略拿快照边界(或 latest);</li>
 *   <li>循环 `poll() → handle(batch) → commitWithBusinessWrite(...)`:位点与"业务写"同事务;</li>
 *   <li>边界情况交给 SDK:位点过期/epoch 变化自动重快照,配额退避重试,死信兜底;</li>
 *   <li>收尾报告:批次数、处理行/事件数、当前位点、是否追平(watermark)。</li>
 * </ol>
 *
 * <p>与真实消费方的唯一差别:这里的"业务写"只是**计数**(演示),真实消费方写自己的目标库。
 * 位点落在 `consumer_offset`(元库),因此**跨进程/跨重启可续**。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutletSdkRunner {

    private final OutletApiFactory apiFactory;
    private final ConsumerOffsetStore offsets;
    private final OutletProperties props;
    private final OutletMetrics metrics;

    /**
     * 跑一轮消费(最多 {@code maxBatches} 批或 {@code maxMillis} 毫秒)。
     *
     * @param startPolicy SNAPSHOT(默认,首次会先做快照) | LATEST
     */
    public Map<String, Object> runOnce(String consumerId, List<String> streams, String startPolicy,
                                       int maxBatches, long maxMillis) {
        OutletConsumer consumer = OutletConsumer.builder(apiFactory.forConsumer(consumerId), offsets)
                .consumerId(consumerId)
                .streams(streams)
                .retry(new RetryPolicy(3, 200, 2000, null))
                .deadLetterSink(new LoggingDeadLetterSink())
                .snapshotLimit(props.getSnapshot() == null ? 1000 : props.getSnapshot().getPageLimit())
                .batchLimit(500)
                .build();

        OutletConsumer.StartPolicy policy = "LATEST".equalsIgnoreCase(startPolicy)
                ? OutletConsumer.StartPolicy.LATEST : OutletConsumer.StartPolicy.SNAPSHOT;
        consumer.open(policy);

        int batches = 0;
        long rows = 0;
        long events = 0;
        long deadline = System.currentTimeMillis() + Math.max(1000, maxMillis);

        while (batches < maxBatches && System.currentTimeMillis() < deadline) {
            SdkBatch b = consumer.poll();
            if (b == null) {
                break;   // 无新数据(已追平或暂空)
            }
            // ★ 位点与"业务写"同事务:这里业务写=计数(演示);真实消费方换成自己的落库
            final SdkBatch batch = b;
            consumer.commitWithBusinessWrite(batch, () -> {
                if (batch.getKind() == SdkBatch.Kind.SNAPSHOT) {
                    log.info("[outlet-sdk-run] 快照批次 stream={} rows={} done={}",
                            batch.getStreamId(), batch.getRows().size(), batch.isSnapshotDone());
                }
            });
            batches++;
            rows += b.getRows().size();
            events += b.getEvents().size();
            // ★ 指标埋点(维度:consumer_id / stream_id / kind)—— 消费任务详情页按消费方读
            metrics.recordSdkBatch(consumerId, b.getStreamId(),
                    b.getKind() == null ? "unknown" : b.getKind().name(),
                    b.getRows().size(), b.getEvents().size());
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("consumerId", consumerId);
        out.put("streams", streams);
        out.put("startPolicy", policy.name());
        out.put("batches", batches);
        out.put("rowsHandled", rows);
        out.put("eventsHandled", events);
        out.put("safeWatermark", consumer.safeWatermark());
        out.put("safeProcessedTso", consumer.safeProcessedTso());
        Map<String, Object> perStream = new LinkedHashMap<>();
        streams.forEach(s -> perStream.put(s, consumer.caughtUp(s)));
        out.put("caughtUp", perStream);
        out.put("state", consumer.stats());
        out.put("offsetStore", "consumer_offset(元库,跨进程可续)");
        return out;
    }
}

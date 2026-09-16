package com.dcp.outlet.control;

import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.List;
import java.util.Map;

/**
 * 批阶段运行器:**不碰 Kafka**,在 T0 视图上按切片读全量,用**同一份 logic** 以
 * {@code mode=SNAPSHOT} 打底。
 *
 * <pre>
 *   plan(slices) → logic.init(一次) → 逐片:readSlice → process(mode=SNAPSHOT) → 全片成功
 *   失败:片内退避重试 → 仍失败则整批失败(调用方**重新快照换新 T0**,不猜起点)
 * </pre>
 */
public class BatchPhaseRunner {

    /** 批运行结果。 */
    public static final class Outcome {
        public final boolean ok;
        public final long t0;
        public final int slices;
        public final int rows;
        public final long durationMillis;
        public final String error;
        public final int failedSlice;

        Outcome(boolean ok, long t0, int slices, int rows, long durationMillis, String error, int failedSlice) {
            this.ok = ok; this.t0 = t0; this.slices = slices; this.rows = rows;
            this.durationMillis = durationMillis; this.error = error; this.failedSlice = failedSlice;
        }

        @Override public String toString() {
            return (ok ? "OK" : "FAILED") + " t0=" + t0 + " slices=" + slices + " rows=" + rows
                    + " " + durationMillis + "ms" + (ok ? "" : " failedSlice=" + failedSlice + " err=" + error);
        }
    }

    private final SnapshotSource source;
    private final SlotPlanner planner;
    private final int sliceRetries;
    private final long retryBackoffMillis;

    public BatchPhaseRunner(SnapshotSource source, SlotPlanner planner, int sliceRetries, long retryBackoffMillis) {
        this.source = source; this.planner = planner;
        this.sliceRetries = sliceRetries; this.retryBackoffMillis = retryBackoffMillis;
    }

    public BatchPhaseRunner(SnapshotSource source) {
        this(source, SlotPlanner.defaults(), 2, 200L);
    }

    public Outcome run(String taskId, String consumerId, String consumerGroup, String stream, String topic,
                       long t0, int partitions, String logicId, String logicVersion, ConsumerLogic logic,
                       Map<String, Object> params, SdkLogger log) {
        long started = System.currentTimeMillis();
        List<Slice> slices = planner.plan(stream, source.estimateRows(consumerId, stream, t0), partitions);
        int rows = 0, processed = 0;
        int[] failed = {-1};
        String error = null;
        try {
            logic.init(new BatchInitContext(taskId, logicVersion, topic, params, log));
            for (Slice slice : slices) {
                if (!processSlice(taskId, consumerId, consumerGroup, stream, t0, logicId, logicVersion,
                        logic, params, log, slice, failed)) {
                    error = "slice " + slice.index() + "/" + slice.total() + " 重试 " + sliceRetries + " 次仍失败";
                    return new Outcome(false, t0, processed, rows, System.currentTimeMillis() - started,
                            error, failed[0]);
                }
                processed++;
                rows += sliceRows;
            }
            return new Outcome(true, t0, processed, rows, System.currentTimeMillis() - started, null, -1);
        } catch (Throwable t) {
            error = String.valueOf(t.getMessage());
            return new Outcome(false, t0, processed, rows, System.currentTimeMillis() - started, error, failed[0]);
        } finally {
            try { logic.close(); } catch (Throwable ignored) { }
        }
    }

    private int sliceRows;      // 最近一片的行数(单线程运行,不做并发改造前够用)

    private boolean processSlice(String taskId, String consumerId, String consumerGroup, String stream, long t0,
                                 String logicId, String logicVersion, ConsumerLogic logic,
                                 Map<String, Object> params, SdkLogger log, Slice slice, int[] failed) {
        for (int attempt = 0; attempt <= sliceRetries; attempt++) {
            try {
                List<ChangeRecord> records = source.readSlice(consumerId, stream, t0, slice);
                sliceRows = records.size();
                if (records.isEmpty()) return true;                     // 空片:不必打扰逻辑
                logic.process(new SnapshotBatch(stream, records, slice.index(), slice.total()),
                        new BatchRunContext(taskId, consumerGroup, t0, params, logicId, logicVersion, log));
                return true;
            } catch (Throwable t) {
                log.warn("批切片失败 attempt {}/{} slice={} : {}", attempt + 1, sliceRetries + 1,
                        slice.index(), String.valueOf(t.getMessage()));
                if (attempt < sliceRetries) sleepQuietly(retryBackoffMillis);
            }
        }
        failed[0] = slice.index();
        return false;
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) return;
        try { Thread.sleep(millis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}

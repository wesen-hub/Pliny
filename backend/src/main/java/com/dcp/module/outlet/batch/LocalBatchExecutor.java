package com.dcp.module.outlet.batch;

import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.ChangeEvent;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.sdk.OutletApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地批执行器(骨架):用出口 L3 接口物化,语义与 Spark 版一致 ——
 * **同一 T0 视图**、**区间 [lo,hi]**、**不可变且按 boundary 版本化的 staging**、**重跑幂等**。
 *
 * <p>与 Spark 版的差异(显式标注):①并行度=1(Spark 版按主键切片并行)②staging 落在内存/本地目录
 * (Spark 版写 Iceberg/对象存储)③不做 spill/资源组(Spark 版由集群承担)。
 */
@Slf4j
@RequiredArgsConstructor
public class LocalBatchExecutor implements BatchExecutor {

    private final OutletApi api;

    /** staging 存储:{stagingPath → 行集合}(生产为 Iceberg/对象存储) */
    private final Map<String, Map<String, Map<String, Object>>> staging = new ConcurrentHashMap<>();
    /** 幂等:同一 stagingPath 重复执行直接复用(不重复搬数) */
    private final Map<String, Integer> execCount = new ConcurrentHashMap<>();

    @Override
    public ExecutionResult bootstrap(BatchJobSpec spec, long boundaryTso, int epoch) {
        String path = stagingPath(spec, SnapshotManifestJobType.BOOTSTRAP, boundaryTso, null);
        if (staging.containsKey(path)) {
            execCount.merge(path, 1, Integer::sum);
            return new ExecutionResult(path, staging.get(path).size(), checksum(staging.get(path)));
        }
        Map<String, Map<String, Object>> table = new LinkedHashMap<>();
        String pageToken = null;
        long total = 0;
        do {
            SnapshotPage page = api.snapshot(spec.getStreamId(), boundaryTso, pageToken, spec.getSnapshotLimit());
            for (Map<String, Object> row : page.getRows()) {
                table.put(String.valueOf(row.get("id")), new LinkedHashMap<>(row));
                total++;
            }
            pageToken = page.getPageToken();
            if (page.isDone()) {
                break;
            }
        } while (true);
        staging.put(path, table);
        execCount.put(path, 1);
        log.info("[outlet-batch] bootstrap 完成 stream={} T0={} rows={} staging={}",
                spec.getStreamId(), boundaryTso, table.size(), path);
        return new ExecutionResult(path, table.size(), checksum(table));
    }

    @Override
    public ExecutionResult incrBatch(BatchJobSpec spec, long loTso, long hiTso, int epoch) {
        String path = stagingPath(spec, SnapshotManifestJobType.INCR_BATCH, loTso, hiTso);
        if (staging.containsKey(path)) {
            execCount.merge(path, 1, Integer::sum);
            return new ExecutionResult(path, staging.get(path).size(), checksum(staging.get(path)));
        }
        Map<String, Map<String, Object>> table = new LinkedHashMap<>();
        String position = com.dcp.module.outlet.model.PositionKey.of(epoch, loTso);
        long scanned = loTso;
        while (true) {
            ChangeBatch batch = api.changes(spec.getStreamId(), position, spec.getBatchLimit());
            for (ChangeEvent e : batch.getEvents()) {
                applyVersioned(table, e);
            }
            scanned = batch.getScannedTo() == null ? scanned : batch.getScannedTo();
            position = batch.getNextPosition();
            if (batch.getEvents().isEmpty() || scanned >= hiTso) {
                break;
            }
        }
        staging.put(path, table);
        execCount.put(path, 1);
        log.info("[outlet-batch] incr_batch 完成 stream={} [{},{}] rows={} staging={}",
                spec.getStreamId(), loTso, hiTso, table.size(), path);
        return new ExecutionResult(path, table.size(), checksum(table));
    }

    /** 版本收敛:只允许更高 TSO 覆盖(与 JdbcIdempotentWriter 的 ODKU 语义一致) */
    private void applyVersioned(Map<String, Map<String, Object>> table, ChangeEvent e) {
        String pk = String.valueOf(e.getKey().getOrDefault("id", e.getRow().get("id")));
        long prev = table.containsKey(pk) ? versionOf(table.get(pk)) : -1;
        if ("DELETE".equals(e.getOp())) {
            table.remove(pk);
            return;
        }
        if (e.getTso() >= prev) {
            Map<String, Object> row = new LinkedHashMap<>(e.getRow());
            row.put("_commit_ts", e.getTso());
            table.put(pk, row);
        }
    }

    private long versionOf(Map<String, Object> row) {
        Object v = row.get("_commit_ts");
        return v == null ? -1 : Long.parseLong(String.valueOf(v));
    }

    /** staging 按 boundary/区间**版本化**:绝不原地覆盖(重跑复用同一路径) */
    private String stagingPath(BatchJobSpec spec, String jobType, Long a, Long b) {
        String range = b == null ? String.valueOf(a) : a + "-" + b;
        return "staging://" + spec.getConsumerId() + "/" + spec.getStreamId() + "/" + jobType + "/" + range;
    }

    private String checksum(Map<String, Map<String, Object>> table) {
        return "rows=" + table.size() + ";pkHash=" + table.keySet().hashCode();
    }

    /* ------------------------------------------------------------------ 观测 */

    public Map<String, Map<String, Map<String, Object>>> stagingView() {
        return staging;
    }

    public int execCount(String stagingPath) {
        return execCount.getOrDefault(stagingPath, 0);
    }

    /** 便于测试:清空(生产不存在这个操作) */
    public void reset() {
        staging.clear();
        execCount.clear();
    }

    public static AtomicInteger counter() {
        return new AtomicInteger();
    }

    public List<String> stagingPaths() {
        return List.copyOf(staging.keySet());
    }

    /** 作业类型常量(避免字符串硬编码) */
    public static final class SnapshotManifestJobType {
        public static final String BOOTSTRAP = "bootstrap";
        public static final String INCR_BATCH = "incr_batch";

        private SnapshotManifestJobType() {}
    }
}

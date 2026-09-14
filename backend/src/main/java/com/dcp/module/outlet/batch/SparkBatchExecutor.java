package com.dcp.module.outlet.batch;

import com.dcp.module.outlet.OutletProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 执行器 B 的 **Spark 版**(设计 §6.3):与 {@link LocalBatchExecutor} 同输入同输出,
 * 差别只在"谁来搬数"——本地执行器自己读出口,Spark 版把活交给集群。
 *
 * <p>产物语义保持一致:同一 T0 视图 / 区间 [lo,hi] / 不可变且版本化 staging / 重跑幂等
 * (幂等由 {@link BatchJobService} 的 manifest 复用保证,Spark 端只允许写新路径)。
 */
@Slf4j
@RequiredArgsConstructor
public class SparkBatchExecutor implements BatchExecutor {

    public interface StagingVerifier {
        /** 作业完成后核对 staging(行数/校验和);dry-run 下返回占位结果 */
        ExecutionResult verify(SparkJobSpec spec);
    }

    private final SparkJobSpecBuilder builder;
    private final SparkLauncher launcher;
    private final StagingVerifier verifier;
    private final OutletProperties props;

    @Override
    public ExecutionResult bootstrap(BatchJobSpec spec, long boundaryTso, int epoch) {
        SparkJobSpec job = builder.build(spec, SnapshotManifestEntityType.BOOTSTRAP, boundaryTso, null, null);
        run(job);
        return verifier.verify(job);
    }

    @Override
    public ExecutionResult incrBatch(BatchJobSpec spec, long loTso, long hiTso, int epoch) {
        SparkJobSpec job = builder.build(spec, SnapshotManifestEntityType.INCR_BATCH, null, loTso, hiTso);
        run(job);
        return verifier.verify(job);
    }

    private void run(SparkJobSpec job) {
        if (props.getSpark() != null && props.getSpark().isDryRun()) {
            log.info("[outlet-batch] dry-run 模式:仅生成规格不提交({})", job.getJobName());
        }
        launcher.launch(job);
    }

    /** 类型常量(与 manifest 的 jobType 对齐) */
    private static final class SnapshotManifestEntityType {
        static final String BOOTSTRAP = "bootstrap";
        static final String INCR_BATCH = "incr_batch";
    }

    /** dry-run 校验器:如实报告"未实际产出",避免把 dry-run 当成成功搬数 */
    public static StagingVerifier dryRunVerifier() {
        return spec -> new ExecutionResult(spec.getStagingPath(), 0L, "dry-run:未实际产出(需 Spark 运行时)");
    }
}

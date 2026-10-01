package com.dcp.module.outlet.batch;

import com.dcp.module.outlet.OutletProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spark 版执行器的规格语义锁(设计 §6.3 六条技术要点)。
 *
 * <p>无 Spark 运行时也能验:断言的是**生成的作业规格与 spark-submit 参数**,
 * 这是"编排放对没放对"的关键;真跑数需要集群(见 README 遗留)。
 */
class SparkJobSpecBuilderTest {

    private final OutletProperties props = new OutletProperties();
    private final SparkJobSpecBuilder builder = new SparkJobSpecBuilder(props);

    private BatchJobSpec spec() {
        BatchJobSpec s = new BatchJobSpec();
        s.setConsumerId("demo-tag");
        s.setStreamId("rt_sync_c");
        s.setJobType("bootstrap");
        s.setTarget("tag_fact_view");
        s.setTransform("tag_rule_v3.sql");
        return s;
    }

    @Test
    @DisplayName("① bootstrap 规格:T0 下发到连接 + GC 校验 + 只经出口读入口 + 原子发布 + 位点收口")
    void bootstrapSpec() {
        long t0 = 469072825121505282L;
        SparkJobSpec job = builder.build(spec(), "bootstrap", t0, null, null);

        String j = job.getJobJson();
        assertTrue(j.contains("\"boundaryTso\":" + t0), "必须带 boundary(T0)");
        assertTrue(j.contains("AS OF TIMESTAMP"), "快照必须走同一 T0 视图");
        assertTrue(j.contains("sessionInitStatement"), "T0 要下发到每个并发连接");
        assertTrue(j.contains("\"gcCheck\":true"), "必须做 GC 越界校验");
        assertTrue(j.contains("\"readViaContract\":true"), "权限不绕:只经出口读入口");
        assertTrue(j.contains("\"manifestRequired\":true"), "产物需 manifest 发布后才可见");
        assertTrue(j.contains("\"commitAfterPublish\":true"), "位点只能在发布后提交");
        assertTrue(job.getStagingPath().contains(String.valueOf(t0)), "staging 按 boundary 版本化");
        assertTrue(job.getArgs().contains("--job-json"), "spark-submit 参数包含作业描述");
    }

    @Test
    @DisplayName("② incr_batch 规格:区间 [lo,hi] + 走 changes 接口(不回表)")
    void incrBatchSpec() {
        SparkJobSpec job = builder.build(spec(), "incr_batch", null, 1000L, 2000L);
        String j = job.getJobJson();
        assertTrue(j.contains("\"loTso\":1000"));
        assertTrue(j.contains("\"hiTso\":2000"));
        assertTrue(j.contains("/changes?after="));
        assertFalse(j.contains("boundaryTso"), "增量批不应带快照边界");
        assertTrue(job.getStagingPath().contains("1000-2000"), "staging 按区间版本化");
    }

    @Test
    @DisplayName("③ 执行器:dry-run 只生成不提交,且如实报告'未实际产出'")
    void executorDryRun() {
        SparkLauncher.DryRun launcher = new SparkLauncher.DryRun();
        SparkBatchExecutor executor = new SparkBatchExecutor(builder, launcher,
                SparkBatchExecutor.dryRunVerifier(), props);

        ExecutionResult r = executor.bootstrap(spec(), 469072825121505282L, 1);
        assertEquals(1, launcher.launched().size(), "应生成 1 个作业规格");
        assertEquals(r.getStagingPath(), launcher.launched().get(0).getStagingPath());
        assertEquals(0L, r.getRowsWritten());
        assertTrue(r.getChecksum().contains("dry-run"), "不得把 dry-run 伪装成搬数成功");
    }
}

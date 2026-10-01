package com.dcp.module.outlet.batch;

/**
 * 批执行器 SPI(设计 §6.3 的可替换点)。
 *
 * <p>骨架实现 {@link LocalBatchExecutor} 用出口 L3 接口在本地物化(便于无 Spark 环境跑通语义);
 * 生产实现应替换为 `spark-submit` 版:**同样的输入(边界 T0 / 区间 [lo,hi])、同样的输出(不可变 staging)**。
 *
 * <p>铁律:执行器**只能经出口契约化只读入口**取数(权限不绕),不得直连数据面裸表。
 */
public interface BatchExecutor {

    /** 全量建仓:在 T0 视图上读全表 → 写不可变 staging */
    ExecutionResult bootstrap(BatchJobSpec spec, long boundaryTso, int epoch);

    /** 增量批:读出口变更事件(带整行,不回表) → upsert 到 staging */
    ExecutionResult incrBatch(BatchJobSpec spec, long loTso, long hiTso, int epoch);
}

package com.dcp.tagjob.run;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.Map;

/**
 * 命中矩阵聚合器(driver 注册的 Spark Accumulator):
 * key 约定 —— "rows": 处理行数; "nullEntity": 实体键为空的行; "hit:{assetIdx}": 该资产命中行数(≥1 动作);
 * "tag:{assetIdx}": 该资产产出的标签行数。assetIdx = plan.assets 下标, 输出时由 driver 对照资产表转可读名。
 */
public final class MatrixAccumulator extends AccumulatorV2<Map<String, Long>, Map<String, Long>> {
    private static final long serialVersionUID = 1L;
    private final Map<String, Long> agg = new HashMap<>();

    @Override
    public void add(Map<String, Long> v) {
        v.forEach((k, x) -> agg.merge(k, x, Long::sum));
    }

    @Override
    public Map<String, Long> value() {
        return new HashMap<>(agg);
    }

    @Override
    public boolean isZero() {
        return agg.isEmpty();
    }

    @Override
    public void reset() {
        agg.clear();
    }

    @Override
    public void merge(AccumulatorV2<Map<String, Long>, Map<String, Long>> other) {
        add(other.value());
    }

    @Override
    public AccumulatorV2<Map<String, Long>, Map<String, Long>> copy() {
        MatrixAccumulator c = new MatrixAccumulator();
        c.agg.putAll(agg);
        return c;
    }
}

package com.dcp.scheduler.model;

/**
 * 实例打分系数配置(运维后台写 /dcp/{ns}/scoring,可选):
 *   score(i) = α·(1 − cpu/100) + β·(1 − assigned/capacity) + γ·affinity + δ·weight
 * 各系数可空,空 = 用缺省值 0.4/0.3/0.2/0.1(调度算法子文档 §三,打分系数"可配")。
 */
public class ScoringConfig {

    public Double alpha;   // 机器 cpu 余量权重(缺省 0.4)
    public Double beta;    // 槽位余量权重(缺省 0.3)
    public Double gamma;   // 亲和性权重(缺省 0.2)
    public Double delta;   // 人工权重权重(缺省 0.1)
}

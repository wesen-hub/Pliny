package com.dcp.scheduler.model;

/** 扩缩规则:ns 默认,任务可逐字段覆盖。 */
public class ScaleRules {

    public double up = 0.8;             // 扩容水位
    public int upPeriods = 3;           // 扩容持续周期
    public double down = 0.4;           // 缩容水位
    public int downPeriods = 6;         // 缩容持续周期
    public int cooldownMinutes = 10;    // 冷却
    public double maxStep = 2.0;        // 单次步长上限(翻倍/减半)
    public boolean downEnabled = true;  // 缩容总开关(ns 级)
}

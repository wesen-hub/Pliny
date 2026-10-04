package com.dcp.scheduler.model;

/**
 * 扩缩规则:ns 默认, 任务可逐字段覆盖。
 *
 * <p>M1: 字段装箱化, null = 未配置(JSON 反序列化缺字段为 null; Gson 不走 Java
 * 字段初始化器, 基本类型缺省会退成 0/false 造成「显式关缩容」与「未配置」无法区分)。
 * 生效值由 Autoscaler.effectiveRules 三层继承: 系统默认 ← ns 规则 ← 任务规则。
 */
public class ScaleRules {

    public Double up;                   // 扩容水位
    public Integer upPeriods;           // 扩容持续周期
    public Double down;                 // 缩容水位
    public Integer downPeriods;         // 缩容持续周期
    public Integer cooldownMinutes;     // 冷却
    public Double maxStep;              // 单次步长上限(翻倍/减半)
    public Boolean downEnabled;         // 缩容总开关(ns 级)
}

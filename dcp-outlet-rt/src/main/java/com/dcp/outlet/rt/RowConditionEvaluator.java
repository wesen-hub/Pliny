package com.dcp.outlet.rt;

import java.util.Map;

/**
 * 行条件求值端口(设计 §7.6 的"行条件"环节)。
 *
 * <p>生产实现接现有数据权限引擎;缺省实现放行(便于平台侧未启用行条件时不影响链路)。
 */
public interface RowConditionEvaluator {

    boolean test(String rowCondition, Map<String, Object> row);
}

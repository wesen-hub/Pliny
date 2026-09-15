package com.dcp.outlet.control;

import java.util.List;

/** 单元存储端口(表 {@code consumer_unit};唯一键 {@code (task, topic, partition, epoch)} 防双消费)。 */
public interface UnitStore {

    void insert(String taskId, List<PlannedUnit> units);

    List<PlannedUnit> byTask(String taskId);

    void markDispatched(String unitId);

    String stateOf(String unitId);
}

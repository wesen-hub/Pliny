package com.dcp.outlet.control.support;

import com.dcp.outlet.control.PlannedUnit;
import com.dcp.outlet.control.UnitStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InMemoryUnitStore implements UnitStore {

    private final Map<String, List<PlannedUnit>> byTask = new LinkedHashMap<>();
    private final Map<String, String> states = new LinkedHashMap<>();

    @Override public void insert(String taskId, List<PlannedUnit> units) {
        byTask.put(taskId, new ArrayList<>(units));
        units.forEach(u -> states.put(u.unitId(), "PENDING"));
    }

    @Override public List<PlannedUnit> byTask(String taskId) {
        return new ArrayList<>(byTask.getOrDefault(taskId, List.of()));
    }

    @Override public void markDispatched(String unitId) { states.put(unitId, "DISPATCHED"); }

    @Override public String stateOf(String unitId) { return states.get(unitId); }
}

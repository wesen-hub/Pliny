package com.dcp.outlet.control.support;

import com.dcp.outlet.control.DispatchRequest;
import com.dcp.outlet.control.UnitDispatcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RecordingDispatcher implements UnitDispatcher {

    private final List<DispatchRequest> dispatched = new ArrayList<>();

    @Override public void dispatch(DispatchRequest request) { dispatched.add(request); }

    public List<DispatchRequest> dispatched() { return List.copyOf(dispatched); }

    public DispatchRequest byUnit(String unitId) {
        return dispatched.stream().filter(d -> d.unitId().equals(unitId)).findFirst().orElse(null);
    }

    public int count() { return dispatched.size(); }

    /** unitId → 该单元各分区起点。 */
    public Map<String, Map<Integer, Long>> startOffsetsByUnit() {
        Map<String, Map<Integer, Long>> out = new LinkedHashMap<>();
        dispatched.forEach(d -> out.put(d.unitId(), d.startOffsets()));
        return out;
    }
}

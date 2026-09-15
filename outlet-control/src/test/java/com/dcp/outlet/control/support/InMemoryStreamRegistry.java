package com.dcp.outlet.control.support;

import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InMemoryStreamRegistry implements StreamRegistry {

    private final Map<String, StreamInfo> byStream = new LinkedHashMap<>();

    @Override public Optional<StreamInfo> get(String stream) { return Optional.ofNullable(byStream.get(stream)); }

    @Override public List<StreamInfo> list() { return List.copyOf(byStream.values()); }

    @Override public void register(StreamInfo info) { byStream.put(info.stream(), info); }
}

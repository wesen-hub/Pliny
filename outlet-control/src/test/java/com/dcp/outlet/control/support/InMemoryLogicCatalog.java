package com.dcp.outlet.control.support;

import com.dcp.outlet.control.LogicCatalog;

import java.util.LinkedHashSet;
import java.util.Set;

public class InMemoryLogicCatalog implements LogicCatalog {

    private final Set<String> registered = new LinkedHashSet<>();

    public InMemoryLogicCatalog register(String logicId, String version) {
        registered.add(logicId + "@" + version);
        return this;
    }

    @Override public boolean registered(String logicId, String version) {
        return registered.contains(logicId + "@" + version);
    }
}

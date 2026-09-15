package com.dcp.outlet.control.support;

import com.dcp.outlet.control.ContractInfo;
import com.dcp.outlet.control.ContractLookup;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class InMemoryContractLookup implements ContractLookup {

    private final Map<String, ContractInfo> byRef = new LinkedHashMap<>();

    public InMemoryContractLookup put(String ref, boolean active, Set<String> streams) {
        byRef.put(ref, new ContractInfo(ref, active, streams));
        return this;
    }

    @Override public Optional<ContractInfo> find(String contractRef) {
        return Optional.ofNullable(byRef.get(contractRef));
    }
}

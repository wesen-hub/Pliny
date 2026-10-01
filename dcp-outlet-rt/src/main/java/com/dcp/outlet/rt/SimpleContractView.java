package com.dcp.outlet.rt;

import com.dcp.outlet.rt.ContractView;

import java.util.Map;
import java.util.Set;

/** 契约视图实现(由控制面下发 / 单测构造)。 */
public class SimpleContractView implements ContractView {

    private final Map<String, Set<String>> columnWhitelist;
    private final Map<String, String> maskRules;
    private final int maxLevel;
    private final String rowCondition;

    public SimpleContractView(Map<String, Set<String>> columnWhitelist, Map<String, String> maskRules,
                              int maxLevel, String rowCondition) {
        this.columnWhitelist = columnWhitelist; this.maskRules = maskRules;
        this.maxLevel = maxLevel; this.rowCondition = rowCondition;
    }

    @Override public Set<String> columnWhitelist(String stream) {
        return columnWhitelist == null ? null : columnWhitelist.get(stream);
    }
    @Override public int maxLevel() { return maxLevel; }
    @Override public Map<String, String> maskRules() { return maskRules == null ? Map.of() : maskRules; }
    @Override public String rowCondition() { return rowCondition; }
}

package com.dcp.config;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * M4: 控制面注解限流规则注册(全内嵌: 规则存 yml dcp.sentinel.flow-rules, 无 Dashboard)。
 *
 * <p>规则格式 "资源名:QPS", 资源名与各 @SentinelResource 的 value 一一对应
 * (api-auth-login / api-cdc-st-control); 超限由各注解的 blockHandler 兜底返回 429。
 */
@Configuration
public class SentinelRulesConfig {

    private final List<String> rawRules;

    public SentinelRulesConfig(@Value("${dcp.sentinel.flow-rules:}") List<String> rawRules) {
        this.rawRules = rawRules;
    }

    @PostConstruct
    public void registerFlowRules() {
        List<FlowRule> rules = new ArrayList<>();
        for (String raw : rawRules) {
            if (raw == null || raw.isBlank()) {
                continue;
            }
            int i = raw.lastIndexOf(':');
            if (i <= 0 || i == raw.length() - 1) {
                continue;
            }
            FlowRule rule = new FlowRule(raw.substring(0, i).trim());
            rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
            rule.setCount(Double.parseDouble(raw.substring(i + 1).trim()));
            rules.add(rule);
        }
        if (!rules.isEmpty()) {
            FlowRuleManager.loadRules(rules);
        }
    }
}

package com.dcp.gateway;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * M4: 网关 Sentinel 限流(全内嵌: 规则存 yml dcp.gateway.sentinel.rules, 无 Dashboard)。
 *
 * <p>规则格式 "路径前缀:QPS"(default 为全量兜底); 请求按最长前缀匹配取资源名,
 * 命中资源按 QPS 限流, 超限返回 429。限流在 JWT 验签(-100)之后: 未认证流量先 401,
 * 不进限流统计。
 *
 * <p>实现要点: SphU.entry 与 exit 在同一线程同步完成(try-with-resources),
 * 只统计 QPS、不跨 reactor 线程持有 Entry, 规避 WebFlux 下 Sentinel ThreadLocal
 * Context 跨线程丢失的问题。
 */
@Component
public class SentinelRateLimitFilter implements GlobalFilter, Ordered {

    private static final String REJECT_BODY = "{\"code\":429,\"message\":\"请求过于频繁, 请稍后重试\"}";
    private static final String DEFAULT_RESOURCE = "default";

    private final List<RuleItem> rules = new ArrayList<>();

    public SentinelRateLimitFilter(@Value("${dcp.gateway.sentinel.rules:}") List<String> rawRules) {
        for (String raw : rawRules) {
            RuleItem item = RuleItem.parse(raw);
            if (item != null) {
                rules.add(item);
            }
        }
    }

    /** 启动时把 yml 规则注册到 FlowRuleManager(全内嵌, 无 Dashboard 推送) */
    @PostConstruct
    public void registerRules() {
        List<FlowRule> flowRules = rules.stream().map(r -> {
            FlowRule rule = new FlowRule(r.prefix);
            rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
            rule.setCount(r.qps);
            return rule;
        }).toList();
        FlowRuleManager.loadRules(flowRules);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String resource = resolveResource(exchange.getRequest().getPath().value());
        try (Entry ignored = SphU.entry(resource)) {
            // 同步 entry/exit: 只统计 QPS(见类注释)
        } catch (BlockException e) {
            return reject(exchange);
        }
        return chain.filter(exchange);
    }

    /** 最长前缀匹配; 无命中走 default 资源 */
    private String resolveResource(String path) {
        String best = null;
        for (RuleItem r : rules) {
            if (path.startsWith(r.prefix) && (best == null || r.prefix.length() > best.length())) {
                best = r.prefix;
            }
        }
        return best == null ? DEFAULT_RESOURCE : best;
    }

    private Mono<Void> reject(ServerWebExchange exchange) {
        ServerHttpResponse resp = exchange.getResponse();
        resp.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        resp.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        DataBuffer buf = resp.bufferFactory().wrap(REJECT_BODY.getBytes(StandardCharsets.UTF_8));
        return resp.writeWith(Mono.just(buf));
    }

    @Override
    public int getOrder() {
        // 晚于 JWT 验签(-100): 先验身份再限流
        return -90;
    }

    /** 限流规则项: 路径前缀 + QPS 阈值 */
    private record RuleItem(String prefix, double qps) {
        static RuleItem parse(String s) {
            if (s == null || s.isBlank()) {
                return null;
            }
            int i = s.lastIndexOf(':');
            if (i <= 0 || i == s.length() - 1) {
                return null;
            }
            try {
                return new RuleItem(s.substring(0, i).trim(), Double.parseDouble(s.substring(i + 1).trim()));
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }
}

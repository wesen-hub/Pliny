package com.dcp.gateway;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * M2: 网关统一 JWT 验签(只验不签; 各后端服务保留自验兜底, 内网直达不裸奔)。
 *
 * <p>白名单(dcp.gateway.auth.whitelist, 格式 METHOD:path)与各后端 SecurityConfig
 * permitAll 对齐: 登录/actuator/机器通道(ingest 提交、agent、变更流数据面、契约拉取)
 * 与订阅方 PULL 由后端自校验(appKey/X-Agent-Secret/X-Dcp-Token/契约), 网关放行;
 * 其余路径一律要求 Authorization: Bearer &lt;token&gt;, 验签失败返回 401。
 * OPTIONS 预检一律放行。
 */
@Component
public class JwtAuthGlobalFilter implements GlobalFilter, Ordered {

    private static final String REJECT_BODY = "{\"code\":401,\"message\":\"未登录或登录已过期\"}";

    private final SecretKey key;
    private final List<Rule> whitelist;
    private final AntPathMatcher matcher = new AntPathMatcher();

    public JwtAuthGlobalFilter(@Value("${dcp.jwt-secret}") String secret,
                               @Value("${dcp.gateway.auth.whitelist:}") List<String> whitelist) {
        byte[] bytes = secret == null ? new byte[0] : secret.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 32) {
            // 与 backend SecurityKeyGuard 同口径: 缺失/短密钥拒绝启动, 不静默放行
            throw new IllegalStateException(
                    "dcp.jwt-secret 未配置或长度不足 32 字节(HS256 最低要求), 请设置环境变量 JWT_SECRET");
        }
        this.key = Keys.hmacShaKeyFor(bytes);
        this.whitelist = whitelist.stream().map(Rule::parse).toList();
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest req = exchange.getRequest();
        HttpMethod method = req.getMethod();
        String path = req.getPath().value();
        if (method == HttpMethod.OPTIONS || whitelisted(method, path)) {
            return chain.filter(exchange);
        }
        String auth = req.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (auth != null && auth.startsWith("Bearer ") && verify(auth.substring(7))) {
            return chain.filter(exchange);
        }
        return reject(exchange);
    }

    /** 验签(jjwt 0.12 语法: verifyWith 抛异常即失败, 含过期/篡改) */
    private boolean verify(String token) {
        try {
            Jwts.parser().verifyWith(key).build().parseSignedClaims(token);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean whitelisted(HttpMethod method, String path) {
        for (Rule r : whitelist) {
            if ((r.method == null || r.method == method) && matcher.match(r.pattern, path)) {
                return true;
            }
        }
        return false;
    }

    private Mono<Void> reject(ServerWebExchange exchange) {
        ServerHttpResponse resp = exchange.getResponse();
        resp.setStatusCode(HttpStatus.UNAUTHORIZED);
        resp.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        DataBuffer buf = resp.bufferFactory().wrap(REJECT_BODY.getBytes(StandardCharsets.UTF_8));
        return resp.writeWith(Mono.just(buf));
    }

    @Override
    public int getOrder() {
        // 早于路由转发(NettyRoutingFilter 等), 未过验签的请求不进入后端
        return -100;
    }

    /** 白名单规则: ANY=不限方法 */
    private record Rule(HttpMethod method, String pattern) {
        static Rule parse(String s) {
            int i = s.indexOf(':');
            String m = i > 0 ? s.substring(0, i) : "ANY";
            String p = i > 0 ? s.substring(i + 1) : s;
            return new Rule("ANY".equalsIgnoreCase(m) ? null : HttpMethod.valueOf(m.toUpperCase()), p);
        }
    }
}

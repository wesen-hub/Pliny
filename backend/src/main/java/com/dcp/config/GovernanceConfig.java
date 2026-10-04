package com.dcp.config;

import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.web.client.RestClient;

/**
 * 微服务治理装配(阶段 A): 服务间调用统一走 Nacos 服务发现负载均衡。
 *
 * <p>用法: 注入本 Builder 后 baseUrl 填服务名(如 http://dcp-worker),
 * 显式填 http://ip:port 时仍直连(本地单机调试/降级), 两者可切换。
 *
 * <p>Builder 是有状态对象(baseUrl/requestFactory 等按客户端配置),
 * 必须 prototype —— singleton 会让多个客户端互相污染配置(中危-4)。
 */
@Configuration
public class GovernanceConfig {

    @Bean
    @LoadBalanced
    @Scope("prototype")
    public RestClient.Builder loadBalancedRestClientBuilder() {
        return RestClient.builder();
    }
}

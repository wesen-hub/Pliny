package com.dcp.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * 微服务治理网关(阶段 A): 唯一外部入口, 前端经 nginx 转发到本网关,
 * 由 lb:// 服务名路由到各控制面服务实例(Nacos 发现)。
 */
@SpringBootApplication
@EnableDiscoveryClient
public class GatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}

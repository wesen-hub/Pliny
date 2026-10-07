package com.dcp.subscription;

import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 数据订阅域独立服务(方案 N11)。
 *
 * <p>承载 backend/module/datasub(订阅契约管理 + pull 数据服务 API + push 定时投递);
 * 管理面(契约 CRUD/审批/投递)走用户 JWT + @perm.isAdmin(与 backend 同套安全套件),
 * 订阅方 PULL 接口(/schema|snapshot|changes|export)走 X-Dcp-Token 自校验, permitAll 放行。
 */
@SpringBootApplication(scanBasePackages = "com.dcp")
@MapperScan("com.dcp.module.system.mapper")  // 主类位于 com.dcp.subscription, 默认 mapper 扫描仅覆盖该子包; 显式扩到 module.system
@EnableDiscoveryClient
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
public class SubscriptionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SubscriptionServiceApplication.class, args);
    }
}

package com.perm.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 策略管理面（端口 8082）
 * 职责：策略 CRUD + 生命周期状态机（R-M1）+ 原子发布六步（R-M3）+ 发布前冲突检测（R-M7/R-M8）
 * 发布事件：进程内 Spring 事件（演示级）；正式形态走 Kafka 广播，PDP 订阅后策略级定点失效。
 */
@SpringBootApplication
public class AdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
    }
}

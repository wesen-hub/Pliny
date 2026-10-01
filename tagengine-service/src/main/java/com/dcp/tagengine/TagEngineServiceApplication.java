package com.dcp.tagengine;

import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 打标引擎域独立服务(方案 N9)。
 *
 * <p>承载 backend/module/tagengine(规则/任务/数据集/标签/在线/流作业/数据权限管理面);
 * 全部接口走用户 JWT + @perm.isAdmin(与 backend 同套安全套件), 仅 actuator permitAll;
 * 任务发布后的 v2 登记(TagV2Deployer)与 Flink 流作业(StreamJobService)均直连 outlet-service REST。
 */
@SpringBootApplication
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
public class TagEngineServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TagEngineServiceApplication.class, args);
    }
}

package com.perm.pdp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * PDP 判定服务（端口 8081）
 * 职责：策略快照加载 → 判定链（DecisionEngine）→ 方言渲染（模板无值 C1）→ HMAC 签名 → 义务审计
 * 快照热更新：@EnableScheduling 驱动 SnapshotSyncService 每 5s 版本比对（管理面联动）
 */
@SpringBootApplication
@EnableScheduling
public class PdpApplication {

    public static void main(String[] args) {
        SpringApplication.run(PdpApplication.class, args);
    }
}

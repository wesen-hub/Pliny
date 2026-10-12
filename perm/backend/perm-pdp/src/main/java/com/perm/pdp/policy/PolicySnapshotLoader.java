package com.perm.pdp.policy;

import com.perm.common.model.Effect;
import com.perm.common.model.MaskType;
import com.perm.common.model.Policy;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.Predicates;
import com.perm.common.model.TagExpandMode;
import com.perm.common.model.Visibility;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 策略快照加载器
 * 演示级：内置示例策略 + 热更新（reload）。正式实现由 perm-admin 发布事件驱动（R-M3 事件广播）。
 * 判定永远基于不可变快照（R-M6），版本号单调递增（R-M4）。
 */
@Component
public class PolicySnapshotLoader {

    /** 当前快照（不可变引用替换） */
    private final AtomicReference<Snapshot> current = new AtomicReference<>(builtinSnapshot());

    public record Snapshot(int version, List<Policy> policies) {
    }

    public Snapshot current() {
        return current.get();
    }

    /** 热更新（版本号 +1） */
    public synchronized Snapshot reload(List<Policy> newPolicies) {
        Snapshot old = current.get();
        return install(newPolicies, old.version() + 1)
                ? current.get() : old;
    }

    /**
     * 安装远端快照（PDP×Admin 联动热更新入口）
     * 版本单调防回退：仅当 version > 当前版本时生效（R-M4 版本语义）。
     * @return 是否安装成功
     */
    public synchronized boolean install(List<Policy> policies, int version) {
        Snapshot old = current.get();
        if (version <= old.version()) {
            return false;
        }
        current.set(new Snapshot(version, List.copyOf(policies)));
        return true;
    }

    /** 内置演示策略（与 perm-common 判定链测试一致） */
    private static Snapshot builtinSnapshot() {
        List<Policy> policies = new ArrayList<>();
        // 区域经理：行级过滤 + amount 掩码 + phone 列裁剪 + sync 审计
        policies.add(Policy.builder()
                .policyId("pol_region_001")
                .policyName("区域经理行权限")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("regional_manager")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.and(
                                Predicates.eq("region", "subject.region"),
                                Predicates.in("status", "subject.allowedStatus")))
                        .columns(List.of("id", "order_no", "customer_id", "region", "amount", "status"))
                        .mask("amount", MaskType.HASH)
                        .build())
                .obligation("AUDIT", "sync")
                .build());
        // 黑名单：张三显式 DENY
        policies.add(Policy.builder()
                .policyId("pol_deny_zhangsan_002")
                .policyName("张三黑名单")
                .effect(Effect.DENY)
                .subjectType("user").subjectId("zhangsan")
                .resourceId("sdm.sales_order")
                .action("select")
                .priority(999)
                .obligation("AUDIT", "sync")
                .build());
        // 营销分析：标签模式 A 列表下推
        policies.add(Policy.builder()
                .policyId("pol_marketing_tag_003")
                .policyName("高价值客户标签权限")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("marketing_analyst")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .tagScope(new PolicyConstraints.TagScope(
                                "业务域/客户价值/高价值", TagExpandMode.LIST,
                                "ts-20260928-001", "customer_id", 2,
                                List.of("101", "102", "103", "104", "105")))
                        .columns(List.of("id", "order_no", "customer_id", "amount", "status"))
                        .build())
                .obligation("AUDIT", "sync")
                .build());
        // 实习生：表级不可见
        policies.add(Policy.builder()
                .policyId("pol_intern_hidden_004")
                .policyName("实习生表级不可见")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("intern")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder().visible(Visibility.HIDDEN).build())
                .build());
        return new Snapshot(1, policies);
    }
}

package com.perm.common.decision;

import com.perm.common.bind.ParamBinder;
import com.perm.common.inject.ColumnPruner;
import com.perm.common.inject.PredicateInjector;
import com.perm.common.inject.TagPushdown;
import com.perm.common.model.Artifact;
import com.perm.common.model.AuthorizationContext;
import com.perm.common.model.AuthorizationRequest;
import com.perm.common.model.Decision;
import com.perm.common.model.Effect;
import com.perm.common.model.MaskType;
import com.perm.common.model.Policy;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.Predicates;
import com.perm.common.model.TagExpandMode;
import com.perm.common.model.Visibility;
import com.perm.common.render.MySqlRenderer;
import com.perm.common.render.PredicateTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 判定链端到端测试（对应 JS 版 demo S1~S8）
 * 覆盖：行谓词 AND 注入 / DENY-overrides / fail-closed / 标签模式 A 下推 /
 *       标签门槛短路 / 表级 HIDDEN / 绑定失败 / 多策略合并
 */
class DecisionEngineTest {

    private static final List<String> SCHEMA =
            List.of("id", "order_no", "customer_id", "region", "amount", "phone", "status");

    private final MySqlRenderer renderer = new MySqlRenderer();
    private List<Policy> policies;

    @BeforeEach
    void setUp() {
        policies = List.of(
                // 区域经理：本区域行级过滤 + amount 掩码 + phone 列裁剪 + sync 审计
                Policy.builder()
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
                        .build(),
                // 黑名单：张三显式 DENY
                Policy.builder()
                        .policyId("pol_deny_zhangsan_002")
                        .policyName("张三黑名单")
                        .effect(Effect.DENY)
                        .subjectType("user").subjectId("zhangsan")
                        .resourceId("sdm.sales_order")
                        .action("select")
                        .priority(999)
                        .obligation("AUDIT", "sync")
                        .build(),
                // 营销分析：标签模式 A 列表下推
                Policy.builder()
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
                        .build(),
                // 实习生：表级不可见
                Policy.builder()
                        .policyId("pol_intern_hidden_004")
                        .policyName("实习生表级不可见")
                        .effect(Effect.GRANT)
                        .subjectType("role").subjectId("intern")
                        .resourceId("sdm.sales_order")
                        .action("select")
                        .constraints(PolicyConstraints.builder().visible(Visibility.HIDDEN).build())
                        .build(),
                // 全列可见 + 行规则：visible=ALL 不应吞掉行谓词（列可见性与行过滤正交）
                Policy.builder()
                        .policyId("pol_all_visible_row_005")
                        .policyName("全列可见但行过滤")
                        .effect(Effect.GRANT)
                        .subjectType("role").subjectId("auditor")
                        .resourceId("sdm.sales_order")
                        .action("select")
                        .constraints(PolicyConstraints.builder()
                                .visible(Visibility.ALL)
                                .rowExpr(Predicates.eq("region", "subject.region"))
                                .build())
                        .build());
    }

    /** PEP 端到端封装（判定 → 渲染 → 绑定 → 注入 → 投影） */
    private Artifact decide(String userId, List<String> roles) {
        return DecisionEngine.decide(
                AuthorizationRequest.builder()
                        .context(AuthorizationContext.builder()
                                .userId(userId)
                                .attribute("region", "华东")
                                .attribute("allowedStatus", List.of("paid", "shipped"))
                                .build())
                        .resourceId("sdm.sales_order")
                        .action("select")
                        .build(),
                policies);
    }

    @Test
    @DisplayName("S1 区域经理：行谓词 AND + 掩码 + 列裁剪")
    void s1RegionalManagerRowFilterMaskPrune() {
        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("lisi").role("regional_manager")
                .attribute("region", "华东")
                .attribute("allowedStatus", List.of("paid", "shipped"))
                .build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                policies);

        assertEquals(Decision.PERMIT, artifact.decision());
        assertEquals(List.of("pol_region_001"), artifact.matchedPolicies());

        // 渲染（模板无值 C1）
        PredicateTemplate template = renderer.renderPredicate(artifact.rowPredicate());
        assertTrue(template.template().contains("`region` = ?"));
        assertTrue(template.template().contains(":list"));

        // 绑定
        List<ParamBinder.Binding> bindings = ParamBinder.bind(template.placeholders(), ctx);
        assertEquals(2, bindings.size());
        assertEquals("华东", bindings.get(0).value());

        // 注入 + 投影
        String permSql = PredicateInjector.renderBoundPredicate(template.template(), bindings);
        String sql = "SELECT * FROM sales_order WHERE order_date >= ?";
        String rewritten = PredicateInjector.injectWhere(sql, permSql);
        rewritten = ColumnPruner.projectColumns(rewritten, artifact, SCHEMA, renderer);

        assertTrue(rewritten.contains("`region` = ?"));
        assertTrue(rewritten.contains("`status` IN (?, ?)"));
        assertTrue(rewritten.contains("SHA2(`amount`, 256) AS `amount`"));
        assertTrue(rewritten.contains("order_date >= ?"));
        // phone 被裁剪（不在可见列）
        assertTrue(!rewritten.contains("phone"));
        // 权限 ? 在业务 ? 之后
        assertTrue(rewritten.indexOf("order_date") < rewritten.indexOf("`region`"));
    }

    @Test
    @DisplayName("S2 黑名单：DENY-overrides")
    void s2DenyOverrides() {
        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("zhangsan").role("regional_manager")
                .attribute("region", "华东")
                .attribute("allowedStatus", List.of("paid", "shipped"))
                .build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                policies);
        assertEquals(Decision.DENY, artifact.decision());
        assertTrue(artifact.denyReason().contains("pol_deny_zhangsan_002"));
        // DENY 也带义务（审计黑名单尝试）
        assertEquals(1, artifact.obligations().size());
    }

    @Test
    @DisplayName("S3 无角色：fail-closed")
    void s3FailClosedNoMatch() {
        AuthorizationContext ctx = AuthorizationContext.builder().userId("zhaoliu").build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                policies);
        assertEquals(Decision.DENY, artifact.decision());
        assertTrue(artifact.denyReason().contains("fail-closed"));
    }

    @Test
    @DisplayName("S4 标签模式 A：列表下推 + snapshotVersion 契约")
    void s4TagModeAListPushdown() {
        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("qianqi").role("marketing_analyst")
                .tagLevel("业务域/客户价值/高价值", 2)
                .build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                policies);

        assertEquals(Decision.PERMIT, artifact.decision());
        assertNotNull(artifact.tagScope());
        assertEquals(TagExpandMode.LIST, artifact.tagScope().expandMode());
        assertEquals("ts-20260928-001", artifact.tagScope().snapshotVersion());

        // 标签谓词下推
        String tagPred = TagPushdown.renderTagPredicate(artifact.tagScope());
        assertEquals("`customer_id` IN (?, ?, ?, ?, ?)", tagPred);

        String sql = "SELECT * FROM sales_order WHERE amount > ?";
        String rewritten = PredicateInjector.injectWhere(sql, tagPred);
        assertTrue(rewritten.contains("`customer_id` IN (?, ?, ?, ?, ?)"));
        assertTrue(rewritten.contains("amount > ?"));
    }

    @Test
    @DisplayName("S5 标签门槛不足：T0 < T2 短路（R-T2）")
    void s5TagGateShortCircuit() {
        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("zhouba").role("marketing_analyst")
                .tagLevel("业务域/客户价值/高价值", 0)
                .build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                policies);
        assertEquals(Decision.DENY, artifact.decision());
        assertTrue(artifact.denyReason().contains("标签门槛不足"));
    }

    @Test
    @DisplayName("S6 表级 HIDDEN 短路（R-D6）")
    void s6TableHiddenShortCircuit() {
        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("sunba").role("intern").build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                policies);
        assertEquals(Decision.DENY, artifact.decision());
        assertTrue(artifact.denyReason().contains("HIDDEN"));
    }

    @Test
    @DisplayName("S8 多策略合并：列交集 − exclude + 掩码最严 + 义务最强")
    void s8MultiPolicyMerge() {
        List<Policy> multi = new java.util.ArrayList<>(policies);
        multi.add(Policy.builder()
                .policyId("pol_finance_005")
                .policyName("财务补充列授权")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("regional_manager")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.rangeLit("amount", 0, 50000))
                        .columns(List.of("id", "order_no", "customer_id", "amount", "status"))
                        .excludeColumn("status")
                        .mask("amount", MaskType.PARTIAL)
                        .build())
                .obligation("AUDIT", "async")
                .build());

        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("lisi").role("regional_manager")
                .attribute("region", "华东")
                .attribute("allowedStatus", List.of("paid", "shipped"))
                .build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                multi);

        assertEquals(Decision.PERMIT, artifact.decision());
        // 列交集 − exclude：region 不在 pol_finance_005 的 columns → 剔除；status 被 exclude
        assertEquals(List.of("id", "order_no", "customer_id", "amount"), artifact.visibleColumns());
        // 掩码最严：HASH 胜 PARTIAL（R-D9）
        assertEquals(MaskType.HASH, artifact.masks().get(0).type());
        // 义务最强：sync 胜 async（R-D16）
        assertEquals("sync", artifact.obligations().get(0).mode());
    }

    @Test
    @DisplayName("S7 绑定失败：缺 region 属性 → BindException（fail-closed）")
    void s7BindFailureFailClosed() {
        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("wujiu").role("regional_manager")
                .build(); // 无 region / allowedStatus
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                policies);
        assertEquals(Decision.PERMIT, artifact.decision());
        PredicateTemplate template = renderer.renderPredicate(artifact.rowPredicate());
        com.perm.common.bind.BindException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.perm.common.bind.BindException.class,
                () -> ParamBinder.bind(template.placeholders(), ctx));
        assertTrue(ex.getMessage().contains("fail-closed"));
    }

    @Test
    @DisplayName("校验式大标签：expandMode=VERIFY 不产谓词（权限锚定圈选动作）")
    void verifyModeProducesNoPredicate() {
        List<Policy> verifyPolicies = List.of(
                Policy.builder()
                        .policyId("pol_big_tag_006")
                        .policyName("全国活跃客户（500万实体校验式）")
                        .effect(Effect.GRANT)
                        .subjectType("role").subjectId("marketing_analyst")
                        .resourceId("sdm.sales_order")
                        .action("select")
                        .constraints(PolicyConstraints.builder()
                                .visible(Visibility.CONDITIONAL)
                                .tagScope(new PolicyConstraints.TagScope(
                                        "业务域/客户价值/全国活跃", TagExpandMode.VERIFY,
                                        "ts-20260928-001", "customer_id", 2, List.of()))
                                .build())
                        .obligation("AUDIT", "sync")
                        .build());
        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("qianqi").role("marketing_analyst")
                .tagLevel("业务域/客户价值/全国活跃", 2)
                .build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                verifyPolicies);
        assertEquals(Decision.PERMIT, artifact.decision());
        assertEquals(TagExpandMode.VERIFY, artifact.tagScope().expandMode());
        // 校验式不产谓词
        assertNull(TagPushdown.renderTagPredicate(artifact.tagScope()));
        assertTrue(TagPushdown.tagBindValues(artifact.tagScope()).isEmpty());
    }

    @Test
    @DisplayName("S9 回归：visible=ALL + 行规则 → 行谓词必须保留（列可见性与行过滤正交）")
    void s9AllVisibleStillKeepsRowPredicate() {
        AuthorizationContext ctx = AuthorizationContext.builder()
                .userId("auditor").role("auditor")
                .attribute("region", "华东")
                .build();
        Artifact artifact = DecisionEngine.decide(
                AuthorizationRequest.builder().context(ctx)
                        .resourceId("sdm.sales_order").action("select").build(),
                policies);

        assertEquals(Decision.PERMIT, artifact.decision());
        assertEquals(List.of("pol_all_visible_row_005"), artifact.matchedPolicies());
        // 核心回归断言：visible=ALL 时行谓词不得被短路丢弃
        assertNotNull(artifact.rowPredicate());
        PredicateTemplate template = renderer.renderPredicate(artifact.rowPredicate());
        assertTrue(template.template().contains("`region` = ?"));
    }

    @Test
    @DisplayName("S10 回归：无 WHERE 且无尾空格 SQL 注入不粘连（sales_orderWHERE 问题）")
    void s10InjectWhereWithoutWhereKeepsSpace() {
        String businessSql = "SELECT id, order_no, region, amount FROM sales_order";
        String rewritten = PredicateInjector.injectWhere(businessSql, "`region` = ?");
        assertTrue(rewritten.contains("sales_order WHERE"), "拼接应含空格: " + rewritten);
        assertTrue(rewritten.contains("FROM sales_order WHERE `region` = ?"));

        // 有 WHERE 无尾空格场景同样不能粘连
        String withWhere = "SELECT id FROM sales_order WHERE status = 'PAID'";
        String rewritten2 = PredicateInjector.injectWhere(withWhere, "`region` = ?");
        assertTrue(rewritten2.contains("sales_order WHERE ("), "拼接应含空格: " + rewritten2);
    }
}

package com.perm.admin.service;

import com.perm.admin.meta.InMemoryMetadataService;
import com.perm.admin.model.PolicyRecord;
import com.perm.admin.model.PolicyStatus;
import com.perm.admin.publisher.PolicyPublishedEvent;
import com.perm.admin.publisher.Publication;
import com.perm.admin.publisher.PublicationService;
import com.perm.admin.publisher.InMemoryPublicationHistory;
import com.perm.admin.store.InMemoryPolicyStore;
import com.perm.common.compile.PolicyCompiler;
import com.perm.common.conflict.ConflictReport;
import com.perm.common.conflict.ConflictType;
import com.perm.common.dto.PolicyDsl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 策略管理服务测试（纯单元）
 * 覆盖：生命周期全链路 / 非法转换 / 冲突检测三级处置（ERROR 阻断、WARNING force、C7/C8）
 *       / 编辑与删除约束 / 原子发布版本递增 + 事件广播
 */
class PolicyServiceTest {

    private PolicyService service;
    private PublicationService publicationService;
    private InMemoryMetadataService metadata;
    private List<PolicyPublishedEvent> events;

    @BeforeEach
    void setUp() {
        events = new CopyOnWriteArrayList<>();
        publicationService = new PublicationService(
                e -> events.add((PolicyPublishedEvent) e), new InMemoryPublicationHistory(),
                java.util.Optional.empty());
        metadata = new InMemoryMetadataService(
                List.of("subject.region", "subject.allowedStatus", "subject.depts", "subject.groups"),
                List.of("业务域/客户价值/高价值", "业务域/客户价值/中价值", "业务域/区域/华东"));
        service = new PolicyService(new InMemoryPolicyStore(), new PolicyCompiler(), publicationService, metadata);
    }

    /** 基础 GRANT 策略 DSL（无行谓词，规避冲突检测干扰） */
    private PolicyDsl baseDsl(String id, String name) {
        PolicyDsl dto = new PolicyDsl();
        dto.setPolicyId(id);
        dto.setPolicyName(name);
        dto.setEffect("GRANT");
        dto.setSubjectType("role");
        dto.setSubjectIds(List.of("regional_manager"));
        dto.setResourceId("sdm.sales_order");
        dto.setActions(List.of("select"));
        return dto;
    }

    private PolicyDsl regionLiteralDsl(String id, String regionValue) {
        PolicyDsl dto = baseDsl(id, "区域行权限-" + id);
        dto.setConstraints(new PolicyDsl.ConstraintDsl(
                "CONDITIONAL", List.of("id", "region"),
                null,
                List.of(new PolicyDsl.RowRuleDsl("region", "EQ", null, regionValue)),
                null));
        return dto;
    }

    private void publish(PolicyDsl dto) {
        service.create(dto, "tester");
        service.submit(dto.getPolicyId());
        service.approve(dto.getPolicyId(), "tester", false);
    }

    @Test
    @DisplayName("生命周期全链路：DRAFT→IN_REVIEW→ACTIVE→DEPRECATED→RETIRED")
    void fullLifecycle() {
        PolicyDsl dto = baseDsl("pol_life_001", "生命周期测试");
        PolicyRecord created = service.create(dto, "tester");
        assertEquals(PolicyStatus.DRAFT, created.status());
        assertEquals(0, created.publishedVersion());

        assertEquals(PolicyStatus.IN_REVIEW, service.submit("pol_life_001").status());
        service.approve("pol_life_001", "tester", false);
        PolicyRecord active = service.get("pol_life_001");
        assertEquals(PolicyStatus.ACTIVE, active.status());
        assertTrue(active.publishedVersion() > 0, "发布后版本号应递增");

        assertEquals(PolicyStatus.DEPRECATED, service.deprecate("pol_life_001").status());
        assertEquals(PolicyStatus.RETIRED, service.retire("pol_life_001").status());

        // 发布记录与事件
        assertEquals(1, publicationService.history().size());
        assertEquals(1, events.size());
        assertEquals("PUBLISH", events.get(0).publication().changeType());
    }

    @Test
    @DisplayName("非法状态转换被拒绝：ACTIVE 不可再 submit")
    void illegalTransitionRejected() {
        PolicyDsl dto = baseDsl("pol_illegal_001", "非法转换测试");
        publish(dto);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> service.submit("pol_illegal_001"));
        assertTrue(e.getMessage().contains("非法状态转换"));
    }

    @Test
    @DisplayName("C2 行谓词互斥：region=华北 vs region=华东 → ERROR 阻断发布")
    void rowPredicateContradictionBlocked() {
        publish(regionLiteralDsl("pol_region_n", "华北"));

        PolicyDsl south = regionLiteralDsl("pol_region_s", "华东");
        service.create(south, "tester");
        service.submit("pol_region_s");

        PolicyService.ConflictBlockedException e = assertThrows(
                PolicyService.ConflictBlockedException.class,
                () -> service.approve("pol_region_s", "tester", false));
        assertTrue(e.report().conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.ROW_PREDICATE_CONTRADICTION),
                "应检测到行谓词互斥 ERROR");
    }

    @Test
    @DisplayName("C1 Effect 对撞：GRANT 与 DENY 并存 → WARNING 需 force 确认")
    void effectCollisionNeedsForce() {
        publish(baseDsl("pol_effect_grant", "授权策略"));

        PolicyDsl deny = baseDsl("pol_effect_deny", "拒绝策略");
        deny.setEffect("DENY");
        deny.setPriority(999);
        service.create(deny, "tester");
        service.submit("pol_effect_deny");

        // 无 force → WARNING 异常
        PolicyService.ConflictWarningException e = assertThrows(
                PolicyService.ConflictWarningException.class,
                () -> service.approve("pol_effect_deny", "tester", false));
        assertTrue(e.report().conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.EFFECT_COLLISION));

        // force=true → 放行
        Publication publication = service.approve("pol_effect_deny", "tester", true);
        assertNotNull(publication);
        assertEquals(PolicyStatus.ACTIVE, service.get("pol_effect_deny").status());
    }

    @Test
    @DisplayName("C8 变量引用失效：未知属性 ref → ERROR 阻断")
    void unknownVarRefBlocked() {
        PolicyDsl dto = baseDsl("pol_badvar_001", "变量失效");
        dto.setConstraints(new PolicyDsl.ConstraintDsl(
                "CONDITIONAL", null, null,
                List.of(new PolicyDsl.RowRuleDsl("region", "EQ", "subject.unknownAttr", null)),
                null));
        service.create(dto, "tester");

        ConflictReport report = service.validate("pol_badvar_001");
        assertTrue(report.blocked());
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.VAR_REF_INVALID));
    }

    @Test
    @DisplayName("C7 标签引用失效：未知 tagPath → ERROR 阻断")
    void unknownTagRefBlocked() {
        PolicyDsl dto = baseDsl("pol_badtag_001", "标签失效");
        dto.setConstraints(new PolicyDsl.ConstraintDsl(
                "CONDITIONAL", null, null, null,
                new PolicyDsl.TagScopeDsl("业务域/不存在的标签", "LIST",
                        "ts-001", "customer_id", 2, List.of("1"))));
        service.create(dto, "tester");

        ConflictReport report = service.validate("pol_badtag_001");
        assertTrue(report.blocked());
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.TAG_REF_INVALID));
    }

    @Test
    @DisplayName("元数据目录热生效：注册新属性后 C8 阻断解除（无需重启）")
    void metadataRegisterHotReload() {
        PolicyDsl dto = baseDsl("pol_hotmeta_001", "目录热生效");
        dto.setConstraints(new PolicyDsl.ConstraintDsl(
                "CONDITIONAL", null, null,
                List.of(new PolicyDsl.RowRuleDsl("region", "EQ", "subject.newChannel", null)),
                null));
        service.create(dto, "tester");

        // 注册前：C8 阻断
        assertTrue(service.validate("pol_hotmeta_001").blocked());

        // 目录注册后（模拟渠道接入）：阻断解除，实时生效
        metadata.registerAttribute("subject.newChannel", "channel-a", "新渠道属性");
        ConflictReport report = service.validate("pol_hotmeta_001");
        assertFalse(report.blocked());
        assertTrue(report.conflicts().stream()
                .noneMatch(c -> c.type() == ConflictType.VAR_REF_INVALID));
    }

    @Test
    @DisplayName("编辑/删除约束：ACTIVE 不可编辑，DRAFT 可删除")
    void editConstraints() {
        PolicyDsl dto = baseDsl("pol_edit_001", "编辑约束");
        publish(dto);

        assertThrows(IllegalStateException.class,
                () -> service.update("pol_edit_001", dto, "tester"));
        assertThrows(IllegalStateException.class,
                () -> service.delete("pol_edit_001"));

        PolicyDsl draft = baseDsl("pol_edit_002", "待删");
        service.create(draft, "tester");
        service.delete("pol_edit_002");
        assertThrows(PolicyService.NotFoundException.class,
                () -> service.get("pol_edit_002"));
    }

    @Test
    @DisplayName("原子发布：版本单调递增 + 每次发布一条事件")
    void versionMonotonicAndEvents() {
        publish(baseDsl("pol_ver_001", "版本一"));
        publish(baseDsl("pol_ver_002", "版本二"));

        List<Publication> history = publicationService.history();
        assertEquals(2, history.size());
        assertTrue(history.get(1).version() > history.get(0).version(),
                "planVersion 应单调递增");
        assertEquals(2, events.size());
        // 每次发布事件携带版本
        assertEquals(history.get(0).version(), events.get(0).publication().version());
        assertEquals(history.get(1).version(), events.get(1).publication().version());
    }

    @Test
    @DisplayName("重复创建与不存在策略处理")
    void duplicateAndNotFound() {
        PolicyDsl dto = baseDsl("pol_dup_001", "重复");
        service.create(dto, "tester");
        assertThrows(IllegalArgumentException.class,
                () -> service.create(dto, "tester"));
        assertThrows(PolicyService.NotFoundException.class,
                () -> service.get("pol_missing_001"));
    }
}

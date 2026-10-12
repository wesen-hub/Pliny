package com.perm.admin.service;

import com.perm.admin.dto.PolicyResponseDto;
import com.perm.admin.meta.MetadataService;
import com.perm.admin.model.PolicyRecord;
import com.perm.admin.model.PolicyStatus;
import com.perm.admin.publisher.Publication;
import com.perm.admin.publisher.PublicationService;
import com.perm.admin.store.PolicyStore;
import com.perm.common.compile.PolicyCompiler;
import com.perm.common.conflict.ConflictDetector;
import com.perm.common.conflict.ConflictReport;
import com.perm.common.dto.PolicyDsl;
import com.perm.common.model.Policy;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * 策略管理服务：CRUD + 生命周期状态机（R-M1）+ 原子发布六步（R-M3）
 *
 * 原子发布（approve 触发，R-M3）：
 *   ① 前置校验（状态合法 / 必填完整）
 *   ② 冲突检测（R-M7/R-M8：ERROR 阻断、WARNING 需 force 确认、INFO 放行）
 *   ③ 锁定（发布锁串行化，同一时刻仅一个发布）
 *   ④ 持久化（状态 ACTIVE + planVersion 递增）
 *   ⑤ 事件广播（PolicyPublishedEvent → PDP 订阅失效）
 *   ⑥ 确认（Publication 存档 + 审计）
 */
@Service
public class PolicyService {

    private final PolicyStore store;
    private final PolicyCompiler compiler;
    private final PublicationService publicationService;
    /** 属性/标签目录（C7/C8 校验输入）；由元数据服务提供，每次检测实时读取（注册后热生效） */
    private final MetadataService metadata;

    public PolicyService(PolicyStore store, PolicyCompiler compiler,
                         PublicationService publicationService, MetadataService metadata) {
        this.store = store;
        this.compiler = compiler;
        this.publicationService = publicationService;
        this.metadata = metadata;
    }

    /** 目录 → 冲突检测 registry（每次实时构建，目录维护后无需重启） */
    private ConflictDetector.AttrRegistry attrRegistry() {
        return ConflictDetector.AttrRegistry.of(metadata.attributeRefs().toArray(String[]::new));
    }

    private ConflictDetector.TagRegistry tagRegistry() {
        return ConflictDetector.TagRegistry.of(metadata.tagPaths().toArray(String[]::new));
    }

    // ============ CRUD（仅 DRAFT 可编辑/删除） ============

    public PolicyRecord create(PolicyDsl dto, String operator) {
        Policy policy = compiler.compile(dto);
        if (store.get(policy.policyId()) != null) {
            throw new IllegalArgumentException("策略已存在: " + policy.policyId());
        }
        return store.put(PolicyRecord.create(policy, dto, operator));
    }

    public PolicyRecord update(String policyId, PolicyDsl dto, String operator) {
        PolicyRecord existing = mustGet(policyId);
        if (!existing.status().editable()) {
            throw new IllegalStateException("仅 DRAFT 状态可编辑，当前: " + existing.status());
        }
        Policy newPolicy = compiler.compile(dto);
        if (!newPolicy.policyId().equals(policyId)) {
            throw new IllegalArgumentException("policyId 不可修改: " + policyId);
        }
        return store.put(existing.withPolicy(newPolicy, dto));
    }

    public void delete(String policyId) {
        PolicyRecord existing = mustGet(policyId);
        if (!PolicyStatus.deletable().contains(existing.status())) {
            throw new IllegalStateException("仅 DRAFT 状态可删除，当前: " + existing.status());
        }
        store.remove(policyId);
    }

    // ============ 状态机（R-M1） ============

    public PolicyRecord submit(String policyId) {
        return transition(policyId, PolicyStatus.IN_REVIEW);
    }

    public PolicyRecord reject(String policyId) {
        return transition(policyId, PolicyStatus.DRAFT);
    }

    public PolicyRecord deprecate(String policyId) {
        return transition(policyId, PolicyStatus.DEPRECATED);
    }

    public PolicyRecord retire(String policyId) {
        return transition(policyId, PolicyStatus.RETIRED);
    }

    /**
     * 审批通过 → 原子发布（R-M3 六步）
     * @param force true = 确认 WARNING 后放行（ERROR 永远阻断）
     */
    public Publication approve(String policyId, String operator, boolean force) {
        PolicyRecord record = mustGet(policyId);
        record.status().assertCanTransitionTo(PolicyStatus.ACTIVE, policyId);

        // ② 冲突检测（R-M7/R-M8）
        ConflictReport report = ConflictDetector.detect(
                record.policy(), activePoliciesExcluding(policyId), attrRegistry(), tagRegistry());
        if (report.blocked()) {
            throw new ConflictBlockedException(report);
        }
        if (!report.warnings().isEmpty() && !force) {
            throw new ConflictWarningException(report);
        }

        // ③ 锁定 → ④ 持久化（发布锁内状态翻转 + 版本递增）
        store.publishLock().lock();
        try {
            PolicyRecord latest = mustGet(policyId);
            latest.status().assertCanTransitionTo(PolicyStatus.ACTIVE, policyId);
            int version = store.nextVersion();
            store.put(latest.withStatus(PolicyStatus.ACTIVE).withPublishedVersion(version));

            // ⑤ 事件广播 + ⑥ 确认存档
            return publicationService.announce(new Publication(
                    version, "PUBLISH", List.of(policyId), operator, Instant.now(), report));
        } finally {
            store.publishLock().unlock();
        }
    }

    /** 冲突检测预览（不落库，提交/审批前自查） */
    public ConflictReport validate(String policyId) {
        PolicyRecord record = mustGet(policyId);
        return ConflictDetector.detect(
                record.policy(), activePoliciesExcluding(policyId), attrRegistry(), tagRegistry());
    }

    // ============ 查询与映射 ============

    public PolicyRecord get(String policyId) {
        return mustGet(policyId);
    }

    public List<PolicyRecord> list() {
        return store.all();
    }

    /** 当前 ACTIVE 策略 DSL（快照接口输出，PDP 侧编译） */
    public List<PolicyDsl> activeDsls() {
        return store.activeRecords().stream()
                .map(PolicyRecord::request)
                .toList();
    }

    /** 当前 planVersion（快照接口 + 发布记录） */
    public int planVersion() {
        return store.planVersion();
    }

    public List<Publication> publications() {
        return publicationService.history();
    }

    public static PolicyResponseDto toResponse(PolicyRecord record) {
        Policy p = record.policy();
        PolicyDsl dto = record.request();
        PolicyDsl.ConstraintDsl constraints = dto.getConstraints();
        return new PolicyResponseDto(
                p.policyId(), p.policyName(), p.effect().name(), p.subjectType(),
                p.subjectIds(), p.resourceId(), p.actions(), p.priority(),
                p.effectivePeriod() == null ? null : p.effectivePeriod().from(),
                p.effectivePeriod() == null ? null : p.effectivePeriod().to(),
                p.constraints().visible().name(),
                p.constraints().columns(),
                constraints == null ? null : constraints.masks(),
                constraints == null ? null : constraints.rowRules(),
                constraints == null ? null : constraints.tagScope(),
                dto.getObligations(),
                record.status().name(), record.createdBy(),
                record.createdAt(), record.updatedAt(), record.publishedVersion());
    }

    // ============ 内部工具 ============

    private PolicyRecord transition(String policyId, PolicyStatus to) {
        PolicyRecord record = mustGet(policyId);
        record.status().assertCanTransitionTo(to, policyId);
        return store.put(record.withStatus(to));
    }

    private PolicyRecord mustGet(String policyId) {
        PolicyRecord record = store.get(policyId);
        if (record == null) {
            throw new NotFoundException("策略不存在: " + policyId);
        }
        return record;
    }

    private List<Policy> activePoliciesExcluding(String policyId) {
        return store.activeRecords().stream()
                .map(PolicyRecord::policy)
                .filter(p -> !p.policyId().equals(policyId))
                .toList();
    }

    /** ERROR 阻断（409，报告随响应返回） */
    public static class ConflictBlockedException extends RuntimeException {
        private final ConflictReport report;

        public ConflictBlockedException(ConflictReport report) {
            super("冲突检测阻断发布（ERROR）: " + report.newPolicyId());
            this.report = report;
        }

        public ConflictReport report() {
            return report;
        }
    }

    /** WARNING 需确认（409，报告随响应返回） */
    public static class ConflictWarningException extends RuntimeException {
        private final ConflictReport report;

        public ConflictWarningException(ConflictReport report) {
            super("存在 WARNING 级冲突，需 force=true 确认发布: " + report.newPolicyId());
            this.report = report;
        }

        public ConflictReport report() {
            return report;
        }
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }
}

package com.perm.admin.controller;

import com.perm.admin.dto.PolicyResponseDto;
import com.perm.admin.publisher.Publication;
import com.perm.admin.service.PolicyService;
import com.perm.common.conflict.ConflictReport;
import com.perm.common.dto.PolicyDsl;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 策略管理接口（REST）
 * 生命周期：DRAFT → IN_REVIEW → ACTIVE → DEPRECATED → RETIRED（R-M1）
 * approve 触发原子发布六步（R-M3）+ 发布前冲突检测（R-M7/R-M8）
 */
@RestController
@RequestMapping("/api/v1/policies")
public class PolicyController {

    private final PolicyService policyService;

    public PolicyController(PolicyService policyService) {
        this.policyService = policyService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PolicyResponseDto create(@RequestBody PolicyDsl dto,
                                    @RequestHeader(name = "X-Operator", defaultValue = "admin") String operator) {
        return PolicyService.toResponse(policyService.create(dto, operator));
    }

    @GetMapping
    public List<PolicyResponseDto> list() {
        return policyService.list().stream().map(PolicyService::toResponse).toList();
    }

    @GetMapping("/{policyId}")
    public PolicyResponseDto get(@PathVariable String policyId) {
        return PolicyService.toResponse(policyService.get(policyId));
    }

    @PutMapping("/{policyId}")
    public PolicyResponseDto update(@PathVariable String policyId,
                                    @RequestBody PolicyDsl dto,
                                    @RequestHeader(name = "X-Operator", defaultValue = "admin") String operator) {
        return PolicyService.toResponse(policyService.update(policyId, dto, operator));
    }

    @DeleteMapping("/{policyId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String policyId) {
        policyService.delete(policyId);
    }

    @PostMapping("/{policyId}/submit")
    public PolicyResponseDto submit(@PathVariable String policyId) {
        return PolicyService.toResponse(policyService.submit(policyId));
    }

    @PostMapping("/{policyId}/reject")
    public PolicyResponseDto reject(@PathVariable String policyId) {
        return PolicyService.toResponse(policyService.reject(policyId));
    }

    /** 审批通过 → 原子发布；force=true 确认 WARNING 后放行 */
    @PostMapping("/{policyId}/approve")
    public Map<String, Object> approve(@PathVariable String policyId,
                                       @RequestHeader(name = "X-Operator", defaultValue = "admin") String operator,
                                       @RequestParam(name = "force", defaultValue = "false") boolean force) {
        Publication publication = policyService.approve(policyId, operator, force);
        return Map.of(
                "published", true,
                "version", publication.version(),
                "changeType", publication.changeType(),
                "policyIds", publication.policyIds(),
                "conflicts", publication.conflictSummary());
    }

    @PostMapping("/{policyId}/deprecate")
    public PolicyResponseDto deprecate(@PathVariable String policyId) {
        return PolicyService.toResponse(policyService.deprecate(policyId));
    }

    @PostMapping("/{policyId}/retire")
    public PolicyResponseDto retire(@PathVariable String policyId) {
        return PolicyService.toResponse(policyService.retire(policyId));
    }

    /** 冲突检测预览（不落库）——包装 blocked（record 方法不参与序列化） */
    @PostMapping("/{policyId}/validate")
    public Map<String, Object> validate(@PathVariable String policyId) {
        ConflictReport report = policyService.validate(policyId);
        return Map.of(
                "newPolicyId", report.newPolicyId(),
                "blocked", report.blocked(),
                "warnings", report.warnings().size(),
                "infos", report.infos().size(),
                "conflicts", report.conflicts());
    }
}

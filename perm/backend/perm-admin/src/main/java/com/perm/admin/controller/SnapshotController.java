package com.perm.admin.controller;

import com.perm.admin.service.PolicyService;
import com.perm.common.dto.PolicyDsl;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 快照接口（PDP 版本比对兜底通道的数据源）
 * PDP 每 5s 轮询比对 version，落后时拉取 ACTIVE 策略 DSL 全量重建（PDP 侧编译）。
 */
@RestController
@RequestMapping("/api/v1/snapshot")
public class SnapshotController {

    private final PolicyService policyService;

    public SnapshotController(PolicyService policyService) {
        this.policyService = policyService;
    }

    @GetMapping
    public Map<String, Object> snapshot() {
        List<PolicyDsl> policies = policyService.activeDsls();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("version", policyService.planVersion());
        body.put("policyCount", policies.size());
        body.put("policies", policies);
        return body;
    }
}

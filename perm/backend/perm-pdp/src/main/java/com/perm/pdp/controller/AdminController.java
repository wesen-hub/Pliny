package com.perm.pdp.controller;

import com.perm.pdp.audit.AuditRecorder;
import com.perm.pdp.policy.PolicySnapshotLoader;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 管理面接口（演示级；正式形态由 perm-admin 承担，PDP 侧只保留最小运维面）
 * GET /api/v1/admin/snapshot：当前策略快照版本
 * GET /api/v1/admin/audit：最近审计记录
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final PolicySnapshotLoader snapshotLoader;
    private final AuditRecorder auditRecorder;

    public AdminController(PolicySnapshotLoader snapshotLoader, AuditRecorder auditRecorder) {
        this.snapshotLoader = snapshotLoader;
        this.auditRecorder = auditRecorder;
    }

    @GetMapping("/snapshot")
    public Map<String, Object> snapshot() {
        PolicySnapshotLoader.Snapshot s = snapshotLoader.current();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("version", s.version());
        body.put("policyCount", s.policies().size());
        body.put("policyIds", s.policies().stream().map(p -> p.policyId()).toList());
        return body;
    }

    @GetMapping("/audit")
    public Map<String, Object> audit(@RequestParam(name = "limit", defaultValue = "20") int limit) {
        List<AuditRecorder.Record> records = auditRecorder.recent(limit);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total", auditRecorder.size());
        body.put("records", records);
        return body;
    }
}

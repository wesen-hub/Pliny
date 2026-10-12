package com.perm.admin.controller;

import com.perm.admin.audit.AuditRecord;
import com.perm.admin.audit.AuditStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 审计查询接口（正式形态审计查询入口，替代 PDP 侧演示级运维视图）
 * GET /api/v1/audit?limit=20：按时间倒序返回最近判定审计流水
 */
@RestController
@RequestMapping("/api/v1/audit")
public class AuditController {

    private final AuditStore store;

    public AuditController(AuditStore store) {
        this.store = store;
    }

    @GetMapping
    public Map<String, Object> audit(@RequestParam(name = "limit", defaultValue = "20") int limit) {
        List<AuditRecord> records = store.recent(Math.max(1, Math.min(limit, 200)));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total", records.size());
        body.put("records", records);
        return body;
    }
}

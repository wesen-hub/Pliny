package com.perm.admin.controller;

import com.perm.admin.service.PolicyService;
import com.perm.common.conflict.ConflictReport;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 异常映射：语义异常 → HTTP 状态码
 * 非法状态转换/冲突阻断 → 409；编译/参数错误 → 400；不存在 → 404
 */
@RestControllerAdvice
public class AdminExceptionHandler {

    @ExceptionHandler(PolicyService.ConflictBlockedException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, Object> conflictBlocked(PolicyService.ConflictBlockedException e) {
        return conflictBody(e.report(), "blocked", e.getMessage());
    }

    @ExceptionHandler(PolicyService.ConflictWarningException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, Object> conflictWarning(PolicyService.ConflictWarningException e) {
        return conflictBody(e.report(), "warning", e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, Object> illegalState(IllegalStateException e) {
        return Map.of("error", "illegal_state", "message", e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> badRequest(IllegalArgumentException e) {
        return Map.of("error", "bad_request", "message", e.getMessage());
    }

    @ExceptionHandler(PolicyService.NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, Object> notFound(PolicyService.NotFoundException e) {
        return Map.of("error", "not_found", "message", e.getMessage());
    }

    private Map<String, Object> conflictBody(ConflictReport report, String level, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "conflict_" + level);
        body.put("message", message);
        body.put("policyId", report.newPolicyId());
        body.put("blocked", report.blocked());
        body.put("conflicts", report.conflicts());
        return body;
    }
}

package com.dcp.scheduler.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * rest 管理 API 异常映射(迁自 backend 的 BizException 语义):
 * IllegalArgumentException → 400(参数/业务校验),IllegalStateException/其他 → 500。
 * 仅作用于本包 controller,不影响宿主其他接口的异常处理。
 */
@RestControllerAdvice(assignableTypes = SchedulerRestController.class)
public class RestExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RestExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<RestResp<Void>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(RestResp.error(400, e.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<RestResp<Void>> internal(IllegalStateException e) {
        log.warn("调度管理 API 内部错误: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(RestResp.error(500, e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<RestResp<Void>> unexpected(Exception e) {
        log.warn("调度管理 API 未预期错误", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(RestResp.error(500, "内部错误: " + e.getMessage()));
    }
}

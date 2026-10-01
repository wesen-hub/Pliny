package com.dcp.module.system.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * 操作审计(delivery-service 精简实现): 写元库 sys_operation_log(与 backend 同表同结构),
 * 无用户认证上下文(变更流数据面为机器消费方), 一律记 system。
 */
@Slf4j
@Service
public class AuditService {

    private final JdbcTemplate metaJdbc;

    public AuditService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    public void record(String module, String operation, String targetType, Long targetId, String detail) {
        try {
            metaJdbc.update("INSERT INTO sys_operation_log(module, operation, target_type, target_id, username, detail) "
                            + "VALUES(?, ?, ?, ?, 'system', ?)",
                    module, operation, targetType, targetId,
                    detail == null ? null
                            : "\"" + detail.replace("\\", "\\\\").replace("\"", "\\\"") + "\"");
        } catch (Exception e) {
            log.warn("审计日志写入失败: {}", e.getMessage());
        }
    }
}

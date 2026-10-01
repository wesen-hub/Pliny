package com.dcp.module.system.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.dcp.module.system.entity.SysOperationLog;
import com.dcp.module.system.mapper.SysOperationLogMapper;
import com.dcp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** 操作审计（S-7：所有写操作落库） */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

    private final SysOperationLogMapper operationLogMapper;

    public void record(String module, String operation, String targetType, Long targetId, String detail) {
        try {
            SysOperationLog logEntry = new SysOperationLog();
            try {
                logEntry.setUserId(SecurityUtils.currentUserId());
                logEntry.setUsername(SecurityUtils.currentUsername());
            } catch (Exception ignored) {
                // 登录等无认证上下文场景, 记录为 system
                logEntry.setUsername("system");
            }
            logEntry.setModule(module);
            logEntry.setOperation(operation);
            logEntry.setTargetType(targetType);
            logEntry.setTargetId(targetId);
            // detail 列是 JSON 类型, 普通文本转义为合法 JSON 字符串
            logEntry.setDetail(detail == null ? null
                    : "\"" + detail.replace("\\", "\\\\").replace("\"", "\\\"") + "\"");
            operationLogMapper.insert(logEntry);
        } catch (Exception e) {
            log.warn("审计日志写入失败: {}", e.getMessage());
        }
    }

    public com.baomidou.mybatisplus.extension.plugins.pagination.Page<SysOperationLog> page(
            int page, int pageSize, String module, String username, String operation) {
        LambdaQueryWrapper<SysOperationLog> qw = new LambdaQueryWrapper<>();
        qw.eq(module != null && !module.isBlank(), SysOperationLog::getModule, module)
          .eq(username != null && !username.isBlank(), SysOperationLog::getUsername, username)
          .eq(operation != null && !operation.isBlank(), SysOperationLog::getOperation, operation)
          .orderByDesc(SysOperationLog::getId);
        return operationLogMapper.selectPage(
                com.baomidou.mybatisplus.extension.plugins.pagination.Page.of(page, pageSize), qw);
    }
}

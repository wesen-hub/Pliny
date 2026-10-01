package com.dcp.common;

import lombok.Getter;

/** 业务错误码（对齐设计文档 §8.1） */
@Getter
public enum ErrorCode {
    SUCCESS(0, "ok"),
    PARAM_ERROR(1001, "参数错误"),
    UNAUTHORIZED(1002, "未认证或登录已过期"),
    FORBIDDEN(1003, "无权限"),
    NOT_FOUND(1004, "资源不存在"),
    VERSION_CONFLICT(1005, "版本冲突"),
    DS_CONN_FAIL(2001, "连接失败"),
    DS_REFERENCED(2002, "数据源被任务引用, 无法删除"),
    DS_META_FAIL(2003, "元数据拉取失败"),
    JOB_CONFIG_INCOMPLETE(3001, "配置不完整"),
    JOB_RUNNING(3002, "任务正在运行"),
    JOB_CRON_INVALID(3003, "调度表达式无效"),
    JOB_INC_FIELD_MISSING(3004, "增量字段缺失"),
    EXPR_COMPILE_FAIL(4001, "表达式编译失败"),
    SQL_VALIDATE_FAIL(4002, "SQL 校验失败"),
    DICT_REFERENCED(4003, "字典被任务引用, 无法删除"),
    ENGINE_SUBMIT_FAIL(5001, "引擎提交失败"),
    ENGINE_TIMEOUT(5002, "执行超时"),
    ENGINE_STOP_FAIL(5003, "停止失败"),
    INTERNAL_ERROR(9000, "内部错误");

    private final int code;
    private final String msg;

    ErrorCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }
}

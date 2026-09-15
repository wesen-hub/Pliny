package com.dcp.outlet.control;

import java.util.List;

/** 建任务校验失败(对应平台错误码 {@code INVALID_TASK});直接抛出全部错误项,便于一次改完。 */
public class TaskValidationException extends RuntimeException {

    private final List<String> errors;

    public TaskValidationException(List<String> errors) {
        super("INVALID_TASK: " + String.join("; ", errors));
        this.errors = List.copyOf(errors);
    }

    public List<String> errors() { return errors; }
}

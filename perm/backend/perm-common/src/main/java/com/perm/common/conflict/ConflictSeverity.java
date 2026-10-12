package com.perm.common.conflict;

/** 冲突严重度分级（R-M7 分级处置） */
public enum ConflictSeverity {
    /** 精确判定，阻断提交（死策略/引用失效） */
    ERROR,
    /** 近似判定，需审批逐条确认（列/掩码/义务覆盖） */
    WARNING,
    /** 不可判定降级，仅提示（复杂谓词/CUSTOM） */
    INFO
}

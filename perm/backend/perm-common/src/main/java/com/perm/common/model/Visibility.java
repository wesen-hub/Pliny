package com.perm.common.model;

/** 表级可见性（R-D6 短路判定输入） */
public enum Visibility {
    /** 全表可见，无行级过滤 */
    ALL,
    /** 行级过滤 */
    CONDITIONAL,
    /** 表级不可见 → 直接 DENY */
    HIDDEN
}

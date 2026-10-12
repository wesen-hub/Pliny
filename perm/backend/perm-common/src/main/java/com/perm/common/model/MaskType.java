package com.perm.common.model;

/**
 * 掩码类型（R-D9 最严掩码：FULL > NULL > HASH > PARTIAL > REPLACE）
 * severity 数值越大越严，冲突合并时取 severity 大者。
 */
public enum MaskType {
    /** 完全不投影 */
    FULL(5),
    /** 置空 */
    NULL(4),
    /** 哈希 */
    HASH(3),
    /** 部分遮蔽 */
    PARTIAL(2),
    /** 替换为固定值 */
    REPLACE(1);

    private final int severity;

    MaskType(int severity) {
        this.severity = severity;
    }

    public int severity() {
        return severity;
    }
}

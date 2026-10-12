package com.perm.common.conflict;

/** 冲突类型（R-M7 九类检测 C1~C9） */
public enum ConflictType {
    /** C1 Effect 对撞：同域内 GRANT 与 DENY 并存 */
    EFFECT_COLLISION,
    /** C2 行谓词互斥（死策略）：值域交集为空 */
    ROW_PREDICATE_CONTRADICTION,
    /** C3 行谓词矛盾合并：一方被另一方完全覆盖（冗余/误导） */
    ROW_PREDICATE_REDUNDANT,
    /** C4 列可见性冲突：同列一条 visible 一条 exclude */
    COLUMN_VISIBILITY_CONFLICT,
    /** C5 掩码冲突：同列不同掩码类型 */
    MASK_CONFLICT,
    /** C6 义务冲突：同域义务不一致 */
    OBLIGATION_CONFLICT,
    /** C7 标签引用失效：TAG_REF 的 tagPath 不存在/门槛不满足 */
    TAG_REF_INVALID,
    /** C8 变量引用失效：rowExpr 的 ref 无法解析 */
    VAR_REF_INVALID,
    /** C9 权限静默收缩：新策略使现有主体权限收窄（影响面分析） */
    PERMISSION_SHRINK
}

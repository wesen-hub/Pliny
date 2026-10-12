package com.perm.common.model;

/** 标签展开模式（R-T7，跨库定稿枚举） */
public enum TagExpandMode {
    /** 模式 A：实体列表下推（实体数 < 1000，跨库唯一 SQL 下推路径） */
    LIST,
    /** 校验式：大标签圈选授权声明（不注入标签谓词，权限锚定圈选动作） */
    VERIFY
}

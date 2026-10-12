package com.perm.admin.meta;

import java.util.List;

/**
 * 元数据服务（R-M 配套：发布前冲突检测 C7/C8 的目录输入）
 * 目录职责：已知主体属性引用（subject.xxx）与标签路径（tagPath）全集，
 * 由三渠道扩展机制（身份/属性/标签）在正式形态下接入企业元数据中心维护；
 * 本接口隔离数据源差异：内存配置（演示默认）或 MySQL 表（jdbc 模式）。
 */
public interface MetadataService {

    /** 已知主体属性引用目录（C8 校验输入），如 subject.region */
    List<String> attributeRefs();

    /** 已知标签路径目录（C7 校验输入），如 业务域/客户价值/高价值 */
    List<String> tagPaths();

    /** 注册新属性引用（渠道接入后扩充目录） */
    void registerAttribute(String ref, String channel, String description);

    /** 注册新标签路径 */
    void registerTagPath(String path, String channel, String description);
}

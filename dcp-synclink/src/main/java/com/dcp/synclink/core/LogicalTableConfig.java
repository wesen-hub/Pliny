package com.dcp.synclink.core;

import lombok.Data;

/**
 * 逻辑表配置视图(业务 jar 自足): 仅含写路径需要的分表规则字段,
 * 与 backend LogicalTableConfigService.LogicalTableConfig 无共享(控制面独立演化)。
 */
@Data
public class LogicalTableConfig {

    private String tableName;
    /** 分表数(<=1 → 逻辑表单物理表) */
    private int tableCount = 50;
    /** 物理表前缀 */
    private String tablePrefix;
}

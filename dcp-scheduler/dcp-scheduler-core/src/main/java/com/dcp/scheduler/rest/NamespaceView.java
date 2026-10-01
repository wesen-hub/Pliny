package com.dcp.scheduler.rest;

import com.dcp.scheduler.model.ScaleRules;

/**
 * namespace 管理视图(运维后台设计 §2.1):ns 列表行 = 名字 + 轻量统计 + 默认规则。
 * ns 无注册表,/dcp/{ns}/ 前缀出现即存在;统计从 etcd 键直读。
 */
public class NamespaceView {

    public String ns;
    public int tasks;          // 任务数(spec 键计数)
    public int instances;      // 在线实例数(注册键计数,不含意图子键)
    public int parts;          // 分区数(ns 大盘聚合)
    public double rps;         // 总 rps(ns 大盘聚合)
    public long lag;           // 总 lag(ns 大盘聚合)
    public boolean hasLeader;  // 是否已有活跃主(leader 键存在)
    public ScaleRules rules;   // 默认扩缩规则(缺失时前端按内置缺省展示)
}

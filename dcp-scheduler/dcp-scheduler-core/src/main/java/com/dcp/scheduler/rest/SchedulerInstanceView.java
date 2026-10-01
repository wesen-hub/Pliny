package com.dcp.scheduler.rest;

/**
 * 实例运维视图:注册键 NsInstanceRecord(/dcp/{ns}/instances/{id})
 * 与主写的意图键(/drain、/paused、/weight)合并——意图优先(键空间注释定论)。
 */
public class SchedulerInstanceView {

    public String id;
    public String status;      // ONLINE/OFFLINE…(注册键)
    public int capacity;       // 本 ns 槽位
    public double weight;      // 调权意图 > 注册键 weight
    public boolean paused;     // 暂停意图存在 或 注册键 paused
    public boolean drained;    // 下架意图存在
}

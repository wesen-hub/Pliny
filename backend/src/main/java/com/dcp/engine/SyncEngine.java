package com.dcp.engine;

/** 同步引擎接口（轻量 JDBC 引擎 / SeaTunnel Zeta 引擎均可实现） */
public interface SyncEngine {

    /** 阻塞执行一次任务（含增量探测/窗口规划），返回结果；失败抛异常由编排器处理重试 */
    EngineResult execute(JobRunContext ctx) throws Exception;
}

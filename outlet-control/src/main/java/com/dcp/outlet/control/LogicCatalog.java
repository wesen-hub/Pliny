package com.dcp.outlet.control;

/**
 * 逻辑目录端口:控制面据此在**建任务时**拦住"逻辑还没注册"的情况
 * (错误码 {@code LOGIC_NOT_AVAILABLE});实际加载失败由计算面另行上报。
 */
public interface LogicCatalog {

    boolean registered(String logicId, String version);
}

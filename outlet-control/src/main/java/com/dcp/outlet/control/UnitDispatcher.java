package com.dcp.outlet.control;

/** 单元下发端口(生产实现 = HTTP/gRPC 到计算面 worker;测试实现 = 内存记录)。 */
public interface UnitDispatcher {

    void dispatch(DispatchRequest request);
}

package com.dcp.outlet.worker;

/** 心跳:上报"我还活着 + 消费到哪了"。平台据此把单元置为 RUNNING,并做停滞判定。 */
public interface Heartbeat {

    void send(String unitId, int partition, long offset, long tso);
}

package com.dcp.scheduler.model;

/** 实例在某 ns 的调度注册(/dcp/{ns}/instances/{id},每归属 ns 一份)。 */
public class NsInstanceRecord {

    public String id;
    public String ns;
    public String status = Enums.InstStatus.ONLINE;
    public int capacity;            // 本 ns 槽位(最大线程数,按 ns 分账)
    public double weight = 1.0;     // 人工调权 0.5~2.0
    public boolean paused;          // 暂停接单(维护窗口)

    public boolean acceptable() {
        return Enums.InstStatus.ONLINE.equals(status) && !paused;
    }

    @Override
    public String toString() {
        return ns + "/" + id + " status=" + status + " capacity=" + capacity + " w=" + weight;
    }
}

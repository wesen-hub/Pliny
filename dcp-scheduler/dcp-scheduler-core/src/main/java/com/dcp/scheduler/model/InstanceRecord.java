package com.dcp.scheduler.model;

import java.util.List;

/** 实例全局身份(/dcp/instances/{id},进程级只一份)。 */
public class InstanceRecord {

    public String id;
    public String addr;
    public List<String> namespaces; // 归属的 ns 列表(启动参数声明,部署属性)
    public Meta meta = new Meta();  // 机器指标(进程级,跨 ns 共享)

    public static class Meta {
        public int cpu;
        public int mem;
        public int load;
        public int jvmThreads;      // JVM 级指标为后续扩展位
    }

    @Override
    public String toString() {
        return "instance=" + id + " addr=" + addr + " ns=" + namespaces;
    }
}

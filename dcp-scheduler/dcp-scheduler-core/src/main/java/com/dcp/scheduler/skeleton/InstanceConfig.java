package com.dcp.scheduler.skeleton;

import com.dcp.scheduler.exec.HookRegistry;
import com.dcp.scheduler.lag.KafkaLagSource;

import java.util.ArrayList;
import java.util.List;

/** 实例启动参数(部署属性,总纲 §八 / 生命周期子文档 §1.4):声明身份与归属 ns 列表。 */
public class InstanceConfig {

    public String id;
    public String addr;
    public String etcd = "http://localhost:2379";

    /** 归属 ns 声明:一个实例可归属多个 ns,每 ns 独立容量/权重/租约/竞选资格。 */
    public List<NsDecl> namespaces = new ArrayList<>();

    public int reconcileSec = 5;      // 对账周期
    public int snapshotSec = 10;      // 快照上报周期
    public long leaseTtlSec = 30;     // 注册/快照租约 TTL
    public long masterDecideSec = 10; // 本实例参选成功后,决策循环周期
    public Integer mockCpu;           // smoke:固定进程 cpu(0-100);null = 真实采集(OS 进程级)
    /** smoke/测试注入的 Kafka endOffset 源;null = 主从 ns 配置懒建内置 Admin。 */
    public KafkaLagSource kafkaLagSource;
    /** 缩容钩子注册表(业务启动期按名注册,主决策时引用);null = 空注册表(无钩子能力,任务只扩不缩)。 */
    public HookRegistry hooks;

    public static class NsDecl {
        public String ns;
        public int capacity = 8;      // 本 ns 槽位(最大并发分区数,按 ns 分账)
        public double weight = 1.0;   // 调权 0.5~2.0
        public boolean paused;        // 暂停接单(维护窗口)
        public boolean campaign;      // 本 ns 是否参选调度主
    }

    public InstanceConfig ns(String ns, int capacity, boolean campaign) {
        NsDecl d = new NsDecl();
        d.ns = ns;
        d.capacity = capacity;
        d.campaign = campaign;
        namespaces.add(d);
        return this;
    }
}

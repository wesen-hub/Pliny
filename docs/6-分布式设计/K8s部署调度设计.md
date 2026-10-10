# K8s 部署调度设计

> 状态:定稿(2026-10-11),基于 K8s 部署形态拍板(域服务全入 K8s、Flink 独立裸部署、跨机房为数据通路而非控制面)。
> 定位:讲清两层调度边界(kube-scheduler 管 Pod、平台调度框架管分区)、静态调度策略(节点池/污点/亲和/优先级)与动态伸缩联动(autoscaler 闭环)的实现细节。
> 配套:入口与网关见《K8s网关请求链路图.svg》;部署总览见《PB级部署方案-总览.md》;调度框架内核见《5-统一调度框架/统一调度框架设计.md》。

## 一、定位与边界:两层调度

```
外层 kube-scheduler:管「Pod 放哪个节点」(worker 实例的物理摆放)
内层 平台调度框架(etcd):管「分区放哪个实例」(业务任务的逻辑归属)
联动点:框架扩缩容建议 → dcp-autoscaler 改副本数 → 新 Pod 进 K8s 调度 → 新实例注册槽位 → 框架再平衡
```

两者正交:worker Pod 是「实例」,分区归属在 etcd 里由调度框架决定(K8s 完全不知道分区存在);kube-scheduler 只负责把实例 Pod 摆到合适的物理节点上。**K8s 是执行器,平台调度框架是决策者**——与网关 MetalLB、worker 槽位注册是同一套分工哲学。

## 二、kube-scheduler 机制

调度器是事件循环:watch 到「nodeName 为空」的 Pod → 进调度队列 → 调度周期(过滤 + 打分)→ 绑定 → kubelet 接管。每个调度周期两阶段:

### 2.1 Filtering(过滤,硬约束,一票否决)

逐节点检查,全过才进候选集:

| 过滤插件 | 语义 |
|---|---|
| NodeResourcesFit | 节点剩余 CPU/内存 ≥ Pod request |
| NodeUnschedulable | 节点未被 cordon |
| TaintToleration | 节点污点(NoSchedule/NoExecute)被 Pod toleration 容忍 |
| NodeAffinity | required 型节点亲和/反亲和满足 |
| PodTopologySpread | 拓扑分布约束满足 |

### 2.2 Scoring(打分,软约束,加权求和)

对候选节点逐项打分(0-100),加权求和,最高分胜出:

| 打分插件 | 依据 |
|---|---|
| NodeResourcesFit(优选) | 资源越贴近需求分越高(均衡摆放,防碎片) |
| ImageLocality | 节点已有该镜像分高(省拉取) |
| NodeAffinity(优选) | preferred 型亲和按距离加分 |
| InterPodAffinity(优选) | 与同池 Pod 同节点按权重罚分 |

### 2.3 绑定

胜出节点写入 Pod `spec.nodeName`,kubelet 拉镜像起容器。全过程「过滤出候选集 → 打分选最优 → 绑定」,与平台调度框架的「三道闸过滤(在线+未暂停+有槽位)→ 打分公式 → CAS 归属」同构——一个是 Pod 维度,一个是分区维度。

## 三、节点池设计(静态分池)

| 池 | 标签 | 污点 | 承载 | 特性 |
|---|---|---|---|---|
| control | `pool=control` | `pool-role=control:NoSchedule` | dcp-control + dcp-infra | 固定,稳定优先 |
| compute | `pool=compute` | 无(默认池) | dcp-runtime(worker 四池) | 弹性伸缩对象 |
| job | `pool=job` | `pool-role=job:NoSchedule` | dcp-job(Spark 批作业) | 共享/可抢占 |

## 四、静态调度策略(Deployment 配对)

| 服务 | nodeSelector | toleration | PriorityClass | 反亲和 |
|---|---|---|---|---|
| 域服务(dcp-control) | pool=control | 容忍 control 污点 | dcp-critical(gateway/权限)/dcp-normal | 2 副本 required |
| worker 池(dcp-runtime) | pool=compute | 无 | dcp-normal | preferred(软) |
| 批作业(dcp-job) | pool=job | 容忍 job 污点 | dcp-batch(可抢占) | 无 |
| 治理面(dcp-infra) | pool=control | 容忍 control 污点 | dcp-normal | preferred |

## 五、PriorityClass 与抢占语义

```yaml
dcp-critical: value=1000, 不可抢占(网关/Nacos/权限——入口稳定优先)
dcp-normal:   value=100
dcp-batch:    value=-1000, 可被抢占
```

效果:批作业错峰跑在共享池,资源紧张时调度器驱逐批作业 Pod 让位给管理面/计算面——批作业「随时可让位」由优先级与抢占机制保证,不靠人工排期。

## 六、弹性伸缩联动(dcp-autoscaler)

### 6.1 闭环时序

```
① 调度框架 ScaleRules 建议写入 etcd ops 键
② dcp-autoscaler(独立小服务,dcp-runtime namespace)watch 到建议
③ 调 K8s API 改 Deployment replicas
④ 新 Pod 进调度队列 → kube-scheduler 按第三、四节规则落到 compute 节点
⑤ worker 启动 → etcd 注册 ns 槽位(lease + capacity/weight)
⑥ 框架下一轮 tick 全量读 → 看到新槽位 → 再平衡分配分区
```

### 6.2 两条纪律

1. **单写**:副本数只由 autoscaler 改;worker 的 CPU 不做 HPA 信号——HPA 与框架建议双写会打架。扩缩容决策权在调度框架(它懂分区与吞吐),K8s 只负责执行;
2. **保底**:每池 minReplicas ≥ 1,防全缩后无实例可注册。

## 七、worker 池 Deployment 完整示例(调度相关字段)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dcp-worker-outlet
  namespace: dcp-runtime
spec:
  replicas: 2                    # 唯一由 dcp-autoscaler 修改的字段
  strategy:
    type: RollingUpdate
    rollingUpdate: { maxUnavailable: 1, maxSurge: 1 }
  template:
    spec:
      nodeSelector: { pool: compute }
      priorityClassName: dcp-normal
      terminationGracePeriodSeconds: 60
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:   # 软反亲和:同池实例不挤节点
          - weight: 100
            podAffinityTerm:
              topologyKey: kubernetes.io/hostname
              labelSelector:
                matchLabels: { app: dcp-worker, pool: outlet }
      containers:
      - name: worker
        image: registry.local/dcp-worker:v2.1.0   # 镜像只含宿主,业务 jar 走 MinIO 拉取
        env:
        - { name: WORKER_NAMESPACES, value: "outlet" }
        - { name: WORKER_CAPACITY,  value: "4" }
        resources:
          requests: { cpu: "4", memory: 8Gi }    # 按容量槽位核算,防超卖
          limits:   { cpu: "8", memory: 16Gi }
        lifecycle:
          preStop: { exec: { command: ["sh", "-c", "sleep 8"] } }
```

## 八、与其他机制的衔接

| 衔接点 | 关系 |
|---|---|
| MetalLB 入口 | VIP 只挂 frontend;worker 无对外暴露 |
| 业务 jar 拉取 | 镜像只含宿主;新任务上线 = 传 jar,不重建镜像(仅逻辑版本化) |
| 优雅停机 | preStop 8s → lease 自然过期前新 Pod 已起,配合框架启动前归属断言 |
| 拓扑同构 | 最小集群与生产组件分布一致,副本数按系数放大 |

## 九、纪律与风险清单

1. 扩缩容单写:replicas 只允许 dcp-autoscaler 修改,任何人工/其他控制器改动视为违规;
2. 池隔离:compute 池不落管理面(污点+nodeSelector 双向约束);批作业不落 control/compute;
3. worker request 按槽位核算,禁止无 request 部署(配合 LimitRange 兜底);
4. 风险:autoscaler 挂掉 → 扩缩容建议积压,但框架 tick 兜底重读,不丢建议;K8s 调度器挂掉 → Pod 悬挂不调度,与平台数据面无关(数据面在裸机)。

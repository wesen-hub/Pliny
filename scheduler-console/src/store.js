import { reactive } from 'vue'

const P = (id, owner, rps, state = 'ASSIGNED') => ({
  id, owner, rps, state,
  lag: Math.round(rps * 0.05 + id * 23),          // 分区维度指标:积压
  latency: +((id % 5) * 4.6 + 5.2).toFixed(1),     // 分区维度指标:处理延迟 ms
  errorRate: +((id % 4) * 0.04).toFixed(2),        // 分区维度指标:错误率 %
})

export const store = reactive({
  namespaces: [
    { name: 'sync', label: '同步链路', desc: '入站链路消费:SeaTunnel 信封 → TiDB', handlerType: 'embedded', status: 'RUNNING', minDef: 4, maxDef: 64, rps: 45800, lag: 3120, tasks: 3, partitions: 16, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: 10 }, scaleDownEnabled: true },
    { name: 'indexer', label: '索引器', desc: '出口链路锚点采样:TiCDC → 锚点表', handlerType: 'embedded', status: 'RUNNING', minDef: 1, maxDef: 16, rps: 96200, lag: 480, tasks: 2, partitions: 6, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: 10 }, scaleDownEnabled: false },
    { name: 'outlet', label: '数据消费', desc: '计算面:单元执行打标 logic', handlerType: 'spi', status: 'RUNNING', minDef: 1, maxDef: 20, rps: 21400, lag: 850, tasks: 4, partitions: 7, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: 10 }, scaleDownEnabled: false },
  ],

  instances: [
    { id: 'sync-01', ns: 'sync', addr: '10.0.8.11', status: 'ONLINE', weight: 1.0, load: 62, partitions: 2 },
    { id: 'sync-02', ns: 'sync', addr: '10.0.8.12', status: 'ONLINE', weight: 1.0, load: 58, partitions: 2 },
    { id: 'sync-03', ns: 'sync', addr: '10.0.8.13', status: 'ONLINE', weight: 1.0, load: 55, partitions: 2 },
    { id: 'sync-04', ns: 'sync', addr: '10.0.8.14', status: 'ONLINE', weight: 1.2, load: 41, partitions: 2 },
    { id: 'sync-05', ns: 'sync', addr: '10.0.8.15', status: 'ONLINE', weight: 1.0, load: 49, partitions: 2 },
    { id: 'sync-06', ns: 'sync', addr: '10.0.8.16', status: 'ONLINE', weight: 1.0, load: 47, partitions: 2 },
    { id: 'sync-07', ns: 'sync', addr: '10.0.8.17', status: 'ONLINE', weight: 1.0, load: 44, partitions: 1 },
    { id: 'sync-08', ns: 'sync', addr: '10.0.8.18', status: 'ONLINE', weight: 1.0, load: 43, partitions: 1 },
    { id: 'sync-09', ns: 'sync', addr: '10.0.8.19', status: 'DRAINING', weight: 1.0, load: 12, partitions: 1 },
    { id: 'sync-10', ns: 'sync', addr: '10.0.8.20', status: 'SUSPECT', weight: 1.0, load: 0, partitions: 1 },
    { id: 'indexer-01', ns: 'indexer', addr: '10.0.9.11', status: 'ONLINE', weight: 1.0, load: 71, partitions: 2 },
    { id: 'indexer-02', ns: 'indexer', addr: '10.0.9.12', status: 'ONLINE', weight: 1.0, load: 68, partitions: 2 },
    { id: 'indexer-03', ns: 'indexer', addr: '10.0.9.13', status: 'ONLINE', weight: 1.0, load: 63, partitions: 2 },
    { id: 'outlet-01', ns: 'outlet', addr: '10.0.10.11', status: 'ONLINE', weight: 1.0, load: 66, partitions: 1 },
    { id: 'outlet-02', ns: 'outlet', addr: '10.0.10.12', status: 'ONLINE', weight: 1.0, load: 64, partitions: 1 },
    { id: 'outlet-03', ns: 'outlet', addr: '10.0.10.13', status: 'ONLINE', weight: 1.0, load: 58, partitions: 1 },
    { id: 'outlet-04', ns: 'outlet', addr: '10.0.10.14', status: 'ONLINE', weight: 1.0, load: 55, partitions: 1 },
    { id: 'outlet-05', ns: 'outlet', addr: '10.0.10.15', status: 'ONLINE', weight: 1.0, load: 52, partitions: 1 },
    { id: 'outlet-06', ns: 'outlet', addr: '10.0.10.16', status: 'ONLINE', weight: 1.0, load: 50, partitions: 1 },
  ].map(i => ({
    ...i,
    cpu: Math.round(i.load * 0.6 + (i.id.charCodeAt(i.id.length - 1) % 5) * 3),  // 实例维度:CPU %
    mem: Math.round(i.load * 0.45 + 32),                                          // 实例维度:内存 %
    jvmThreads: 24 + i.partitions * 6,                                            // 实例维度:JVM 线程数(后续扩展 GC/堆等)
  })),

  tasks: [
    {
      topic: 'dcp.cdc.orders', ns: 'sync', handlerRef: 'embedded:sync-writer', handover: 'AGGRESSIVE',
      min: 8, max: 64, targetRps: 15000, hooks: true, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: '10min' },
      status: 'RUNNING', currentRps: 92000, partitions: [
        P(0, 'sync-01', 11800), P(1, 'sync-02', 11600), P(2, 'sync-03', 11500), P(3, 'sync-04', 11400),
        P(4, 'sync-05', 11300), P(5, 'sync-06', 11600), P(6, 'sync-07', 11400), P(7, 'sync-08', 11400),
      ],
    },
    {
      topic: 'dcp.cdc.payments', ns: 'sync', handlerRef: 'embedded:sync-writer', handover: 'AGGRESSIVE',
      min: 4, max: 32, targetRps: 15000, hooks: true, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: '10min' },
      status: 'RUNNING', currentRps: 41000, partitions: [
        P(0, 'sync-01', 10200), P(1, 'sync-02', 10400), P(2, 'sync-03', 9800), P(3, 'sync-04', 10600),
      ],
    },
    {
      topic: 'dcp.cdc.users', ns: 'sync', handlerRef: 'embedded:sync-writer', handover: 'AGGRESSIVE',
      min: 4, max: 16, targetRps: 15000, hooks: false, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: '10min' },
      status: 'SCALE_DOWN', currentRps: 9800, partitions: [
        P(0, 'sync-05', 2600), P(1, 'sync-06', 2500), P(2, 'sync-07', 2400), P(3, 'sync-08', 2300),
      ],
    },
    {
      topic: 'dcp.cdc.target_db.orders', ns: 'indexer', handlerRef: 'embedded:anchor-sampler', handover: 'AGGRESSIVE',
      min: 1, max: 16, targetRps: 100000, hooks: false, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: '10min' },
      status: 'RUNNING', currentRps: 71200, partitions: [
        P(0, 'indexer-01', 17800), P(1, 'indexer-02', 18000), P(2, 'indexer-03', 17600), P(3, 'indexer-01', 17800),
      ],
    },
    {
      topic: 'dcp.cdc.target_db.payments', ns: 'indexer', handlerRef: 'embedded:anchor-sampler', handover: 'AGGRESSIVE',
      min: 1, max: 8, targetRps: 100000, hooks: false, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: '10min' },
      status: 'RUNNING', currentRps: 25000, partitions: [
        P(0, 'indexer-02', 12500), P(1, 'indexer-03', 12500),
      ],
    },
    {
      topic: 'dcp.cdc.target_db.orders', ns: 'outlet', handlerRef: 'spi:tag-logic-v2.3', handover: 'CONSERVATIVE',
      min: 3, max: 3, targetRps: 6000, hooks: false, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: '10min' },
      status: 'RUNNING', currentRps: 14200, unit: 'unit-orders-001', unitParts: [0, 1, 2], partitions: [
        P(0, 'outlet-01', 4900), P(1, 'outlet-02', 4700), P(2, 'outlet-03', 4600),
      ],
    },
    {
      topic: 'dcp.cdc.target_db.orders', ns: 'outlet', handlerRef: 'spi:tag-logic-v2.3', handover: 'CONSERVATIVE',
      min: 1, max: 1, targetRps: 6000, hooks: false, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: '10min' },
      status: 'RUNNING', currentRps: 4700, unit: 'unit-orders-002', unitParts: [3], partitions: [
        P(3, 'outlet-04', 4700),
      ],
    },
    {
      topic: 'dcp.cdc.target_db.payments', ns: 'outlet', handlerRef: 'spi:tag-logic-v1.8', handover: 'CONSERVATIVE',
      min: 2, max: 2, targetRps: 6000, hooks: false, scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: '10min' },
      status: 'RUNNING', currentRps: 6200, unit: 'unit-payments-001', unitParts: [0, 1], partitions: [
        P(0, 'outlet-05', 3100), P(1, 'outlet-06', 3100),
      ],
    },
  ],

  scalingEvents: [
    { time: '09-29 10:42', ns: 'sync', topic: 'dcp.cdc.orders', action: '自动扩容 6→8 分区', detail: '连续 3 周期负载 87% > 80% 阈值', result: '成功', by: 'auto' },
    { time: '09-29 09:15', ns: 'sync', topic: 'dcp.cdc.users', action: '逻辑缩容 8→4 分区', detail: 'bucket_map 收缩 → 追平 → 停线程', result: '成功', by: 'auto' },
    { time: '09-28 22:30', ns: 'indexer', topic: 'dcp.cdc.target_db.orders', action: '宕机迁移 sync-10 → sync-07', detail: 'sync-10 lease 过期,2 分区重分配', result: '成功', by: 'auto' },
    { time: '09-28 18:03', ns: 'outlet', topic: 'dcp.cdc.target_db.orders', action: '单元接入 unit-orders-002', detail: '规划单元 [3] 起点 ts=xxx', result: '成功', by: 'auto' },
    { time: '09-28 14:20', ns: 'sync', topic: 'dcp.cdc.orders', action: '手动迁移 分区5 → sync-06', detail: '运维指令 ops-1024', result: '成功', by: 'admin' },
  ],

  audits: [
    { time: '09-29 11:02', user: 'admin', ns: 'sync', action: '手动迁移', detail: 'dcp.cdc.orders 分区5:sync-05→sync-06' },
    { time: '09-29 10:55', user: 'admin', ns: 'sync', action: '实例下架', detail: 'sync-09 进入 DRAINING,迁空后下线' },
    { time: '09-29 10:31', user: 'ops-lin', ns: 'indexer', action: '锁定分区', detail: 'dcp.cdc.target_db.payments 分区1 冻结自动调度' },
    { time: '09-28 18:40', user: 'admin', ns: 'outlet', action: '调权重', detail: 'outlet-01 权重 1.0→1.5' },
  ],

  // 缩容流程演示状态
  scaleDownDemo: null,
})

export const nsColor = { sync: '#2563eb', indexer: '#ea580c', outlet: '#059669' }
export const stateLabel = { ASSIGNED: '已分配', PENDING: '待分配', MIGRATING: '迁移中', DELETING: '回收中' }
export const handoverLabel = { AGGRESSIVE: '激进', CONSERVATIVE: '保守' }

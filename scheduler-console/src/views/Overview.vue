<script setup>
import { store, nsColor } from '../store'
</script>

<template>
  <div>
    <h1 class="page-title">总览 · 统一调度框架</h1>
    <p class="page-desc">基于 etcd 的通用"分区调度 + 线程托管"系统:回答"哪个分区在哪个实例的哪个线程上跑",三个消费场景各占一个 namespace,一套框架共用。</p>

    <!-- 架构简图 -->
    <div class="card" style="margin-bottom:16px">
      <svg viewBox="0 0 1160 320" width="100%">
        <defs>
          <marker id="ar" markerWidth="9" markerHeight="7" refX="8" refY="3.5" orient="auto">
            <path d="M0,0 L9,3.5 L0,7 z" fill="#94a3b8"/>
          </marker>
        </defs>
        <!-- 业务外壳 -->
        <rect x="20" y="40" width="250" height="130" rx="10" fill="#eff6ff" stroke="#2563eb" stroke-width="1.5"/>
        <text x="145" y="68" text-anchor="middle" font-size="15" font-weight="bold" fill="#1d4ed8">业务外壳(无状态)</text>
        <text x="145" y="95" text-anchor="middle" font-size="12" fill="#334155">逻辑表保存 / 流扫描新表</text>
        <text x="145" y="115" text-anchor="middle" font-size="12" fill="#334155">单元规划 → 提交 TaskSpec</text>
        <text x="145" y="140" text-anchor="middle" font-size="12" fill="#64748b">只写 etcd,不知道主是谁</text>
        <line x1="270" y1="105" x2="330" y2="105" stroke="#94a3b8" stroke-width="2" marker-end="url(#ar)"/>

        <!-- etcd -->
        <rect x="335" y="30" width="300" height="150" rx="10" fill="#fdf4ff" stroke="#a855f7" stroke-width="1.5"/>
        <text x="485" y="58" text-anchor="middle" font-size="15" font-weight="bold" fill="#7e22ce">etcd(唯一事实源)</text>
        <text x="485" y="82" text-anchor="middle" font-size="11.5" fill="#334155">/dcp/{ns}/leader 每 ns 独立选主</text>
        <text x="485" y="100" text-anchor="middle" font-size="11.5" fill="#334155">/dcp/{ns}/instances 实例(lease 心跳)</text>
        <text x="485" y="118" text-anchor="middle" font-size="11.5" fill="#334155">/dcp/{ns}/tasks 分区归属(PENDING/ASSIGNED)</text>
        <text x="485" y="136" text-anchor="middle" font-size="11.5" fill="#334155">/dcp/{ns}/metrics rules ops</text>
        <text x="485" y="160" text-anchor="middle" font-size="11.5" fill="#7e22ce">namespace 前缀硬隔离,互不可见</text>
        <line x1="635" y1="70" x2="700" y2="70" stroke="#94a3b8" stroke-width="2" marker-end="url(#ar)"/>
        <line x1="635" y1="140" x2="700" y2="140" stroke="#94a3b8" stroke-width="2" marker-end="url(#ar)"/>

        <!-- 调度主 -->
        <rect x="705" y="30" width="210" height="80" rx="10" fill="#fefce8" stroke="#ca8a04" stroke-width="1.5"/>
        <text x="810" y="56" text-anchor="middle" font-size="14" font-weight="bold" fill="#854d0e">调度主(无状态决策器)</text>
        <text x="810" y="76" text-anchor="middle" font-size="11.5" fill="#334155">决策循环:分配/迁移/再平衡</text>
        <text x="810" y="94" text-anchor="middle" font-size="11.5" fill="#334155">扩缩容编排/人工指令/对账</text>

        <!-- 实例 -->
        <rect x="705" y="120" width="210" height="80" rx="10" fill="#f0fdf4" stroke="#16a34a" stroke-width="1.5"/>
        <text x="810" y="146" text-anchor="middle" font-size="14" font-weight="bold" fill="#166534">实例 × N(无状态容器)</text>
        <text x="810" y="166" text-anchor="middle" font-size="11.5" fill="#334155">watch 对账 diff → 起/停线程</text>
        <text x="810" y="184" text-anchor="middle" font-size="11.5" fill="#334155">指标聚合 / 快照上报 / keepalive</text>
        <line x1="915" y1="160" x2="990" y2="160" stroke="#94a3b8" stroke-width="2" marker-end="url(#ar)"/>

        <!-- 执行器 -->
        <rect x="995" y="110" width="140" height="100" rx="10" fill="#ffffff" stroke="#475569" stroke-width="1.5" stroke-dasharray="5,3"/>
        <text x="1065" y="140" text-anchor="middle" font-size="13.5" font-weight="bold" fill="#334155">执行器(业务)</text>
        <text x="1065" y="162" text-anchor="middle" font-size="11" fill="#64748b">poll → 业务 → 提交</text>
        <text x="1065" y="180" text-anchor="middle" font-size="11" fill="#64748b">embedded / spi</text>
        <text x="1065" y="198" text-anchor="middle" font-size="11" fill="#64748b">框架不感知内容</text>

        <!-- 监控后台 -->
        <rect x="705" y="225" width="210" height="70" rx="10" fill="#fff1f2" stroke="#e11d48" stroke-width="1.5"/>
        <text x="810" y="252" text-anchor="middle" font-size="14" font-weight="bold" fill="#9f1239">监控 + 运维后台</text>
        <text x="810" y="272" text-anchor="middle" font-size="11.5" fill="#334155">派生指标 / 告警 / 人工指令(审计)</text>
        <line x1="810" y1="200" x2="810" y2="222" stroke="#94a3b8" stroke-width="2" marker-end="url(#ar)"/>
      </svg>
    </div>

    <!-- ns 卡片 -->
    <div class="ns-grid">
      <div v-for="ns in store.namespaces" :key="ns.name" class="ns-card" :style="{ borderTopColor: nsColor[ns.name] }">
        <div class="ns-head">
          <span class="ns-name" :style="{ color: nsColor[ns.name] }">{{ ns.label }}</span>
          <el-tag size="small" :style="{ color: nsColor[ns.name], borderColor: nsColor[ns.name], background: '#fff' }">ns={{ ns.name }}</el-tag>
        </div>
        <div class="ns-desc">{{ ns.desc }}</div>
        <div class="ns-metrics">
          <div class="metric"><div class="m-val">{{ ns.tasks }}</div><div class="m-label">任务</div></div>
          <div class="metric"><div class="m-val">{{ store.instances.filter(i => i.ns === ns.name).length }}</div><div class="m-label">实例</div></div>
          <div class="metric"><div class="m-val">{{ ns.partitions }}</div><div class="m-label">分区</div></div>
          <div class="metric"><div class="m-val">{{ (ns.rps / 1000).toFixed(1) }}k</div><div class="m-label">rps</div></div>
          <div class="metric"><div class="m-val">{{ ns.lag }}</div><div class="m-label">总 lag</div></div>
        </div>
        <div class="ns-foot">
          <el-tag size="small" type="success">独立选主</el-tag>
          <el-tag size="small" type="info">逻辑装载:{{ ns.handlerType }}</el-tag>
        </div>
      </div>
    </div>

    <!-- 设计要点 -->
    <div class="card">
      <h3 class="card-title">设计要点</h3>
      <div class="points">
        <div class="point"><b>边界</b>框架管:分区归属决策 · 线程生命周期 · 扩缩容编排;业务交:TaskSpec 契约 · 执行器 · 缩容钩子</div>
        <div class="point"><b>调度单位</b>单分区 = 一线程;打分 = 负载比率 + 同 topic 反亲和 + 迁移惩罚;分配用 etcd CAS 防双决策</div>
        <div class="point"><b>无状态化</b>状态全在 etcd,进度全在 Kafka 位点,逻辑全在 handler —— 任何组件重启拉一遍状态即恢复</div>
        <div class="point"><b>交接模式</b>激进(幂等吸收,迁移直接接管)/ 保守(租约确认死亡);assign 直配分区,消除 rebalance 与调度双决策冲突</div>
        <div class="point"><b>监控</b>业务只报 4 个原始计数器(条数/位点/错误/耗时);框架差值计算 rps · lag · errorRate · latency</div>
        <div class="point"><b>扩缩容</b>阈值连续判定 + 冷却期 + 渐进步长;Kafka 只能扩分区,缩容 = 钩子切路由 → 追平 → 停线程</div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.page-title { font-size: 22px; font-weight: 700; color: #0f172a; }
.page-desc { color: #64748b; font-size: 13px; margin: 8px 0 18px; }
.card { background: #fff; border-radius: 12px; padding: 18px 20px; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.card-title { font-size: 15px; color: #0f172a; margin-bottom: 12px; }
.ns-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 16px; }
.ns-card { background: #fff; border-radius: 12px; padding: 16px 18px; border-top: 4px solid; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.ns-head { display: flex; justify-content: space-between; align-items: center; }
.ns-name { font-size: 16px; font-weight: 700; }
.ns-desc { font-size: 12px; color: #64748b; margin: 6px 0 14px; }
.ns-metrics { display: flex; justify-content: space-between; margin-bottom: 14px; }
.metric { text-align: center; flex: 1; }
.m-val { font-size: 19px; font-weight: 700; color: #0f172a; }
.m-label { font-size: 11px; color: #94a3b8; margin-top: 2px; }
.ns-foot { display: flex; gap: 8px; }
.points { display: grid; grid-template-columns: 1fr 1fr; gap: 10px 24px; }
.point { font-size: 12.5px; color: #475569; line-height: 1.7; }
.point b { color: #0f172a; }
</style>

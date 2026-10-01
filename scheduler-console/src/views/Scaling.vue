<script setup>
import { ref, computed } from 'vue'
import { store, nsColor } from '../store'
import { ElMessage } from 'element-plus'

const demo = ref(null) // { task, step: 1..3 }

function startDemo(task) {
  demo.value = { task, step: 1 }
  ElMessage.info('演示:缩容流程启动——调任务声明的业务钩子切换生产端路由')
}

const demoSteps = computed(() => {
  if (!demo.value) return []
  return [
    { label: '① 调业务钩子切路由(任务维度)', desc: '调该任务 TaskSpec 声明的 ScaleDownHooks:生产端停止向回收分区写入(如 bucket_map 收缩)', active: demo.value.step >= 1 },
    { label: '② 追平判定(框架编排)', desc: '回收分区标记 DELETING,执行器继续消费遗留数据,框架监控 lag 归零(或兜底超时)', active: demo.value.step >= 2 },
    { label: '③ 停线程 + 清归属(框架编排)', desc: 'executor.stop() 回收线程,删除 etcd 归属记录,物理分区保留空置,进入冷却期', active: demo.value.step >= 3 },
  ]
})

function nextStep() {
  if (!demo.value) return
  if (demo.value.step < 3) demo.value.step++
  else {
    demo.value.task.partitions.pop()
    demo.value.task.currentRps = Math.round(demo.value.task.currentRps * 0.75)
    store.scalingEvents.unshift({
      time: '09-29 14:20', ns: demo.value.task.ns, topic: demo.value.task.topic,
      action: `手动缩容 ${demo.value.task.partitions.length + 1}→${demo.value.task.partitions.length} 分区`,
      detail: '钩子切路由 → 追平 → 停线程,全部完成', result: '成功', by: 'admin',
    })
    demo.value = null
    ElMessage.success('缩容完成:线程已回收,物理分区空置(Kafka 分区只增不减)')
  }
}
</script>

<template>
  <div>
    <h1 class="page-title">扩缩容</h1>
    <p class="page-desc">
      维度划分:<b>钩子 = 任务维度</b>(业务代码,任务提交时声明)· <b>规则 = ns 默认 + 任务覆盖</b> · <b>缩容开关 = ns 总开关 + 任务"只扩不缩"声明</b>。自动扩缩 = 阈值连续判定 + 冷却期 + 渐进步长。
    </p>

    <!-- ns 级默认规则 -->
    <h3 class="sec">Namespace 级默认规则(任务缺省值,任务可覆盖)</h3>
    <div class="ns-rule-grid">
      <div v-for="ns in store.namespaces" :key="ns.name" class="ns-rule-card" :style="{ borderTopColor: nsColor[ns.name] || '#94a3b8' }">
        <div class="nr-head">
          <b :style="{ color: nsColor[ns.name] || '#0f172a' }">{{ ns.label }}({{ ns.name }})</b>
          <el-tag size="small" type="info">ns 默认</el-tag>
        </div>
        <div class="nr-row"><span>扩容水位</span><b>&gt;{{ Math.round(ns.scaleRules.up * 100) }}% × {{ ns.scaleRules.upPeriods }} 周期</b></div>
        <div class="nr-row"><span>缩容水位</span><b>&lt;{{ Math.round(ns.scaleRules.down * 100) }}% × {{ ns.scaleRules.downPeriods }} 周期</b></div>
        <div class="nr-row"><span>冷却期</span><b>{{ ns.scaleRules.cooldown }} min</b></div>
        <div class="nr-row"><span>默认边界</span><b>[{{ ns.minDef }}, {{ ns.maxDef }}]</b></div>
        <div class="nr-switch">
          <span>缩容总开关</span>
          <el-switch v-model="ns.scaleDownEnabled" size="small" @change="v => ElMessage.success(`演示:${ns.name} 缩容总开关 → ${v ? '开启' : '关闭'}`)" />
        </div>
      </div>
    </div>

    <div class="two-col">
      <!-- 任务级规则 -->
      <div class="card">
        <h3 class="card-title">任务级规则与钩子声明(TaskSpec 提交时确定)</h3>
        <div v-for="t in store.tasks" :key="t.unit || t.ns + t.topic" class="rule-row">
          <div class="rule-head">
            <span class="ns-dot" :style="{ background: nsColor[t.ns] }"></span>
            <b class="rule-topic">{{ t.topic }}</b>
            <span v-if="t.unit" class="unit-tag">单元</span>
            <span class="rule-ns" :style="{ color: nsColor[t.ns] }">{{ t.ns }}</span>
          </div>
          <div class="rule-body">
            <span class="rule-item">规则:&gt;{{ Math.round(t.scaleRules.up * 100) }}% × {{ t.scaleRules.upPeriods }} / &lt;{{ Math.round(t.scaleRules.down * 100) }}% × {{ t.scaleRules.downPeriods }}</span>
            <span class="rule-item">边界 [{{ t.min }}, {{ t.max }}]</span>
            <span class="rule-item">冷却 {{ t.scaleRules.cooldown }}min</span>
            <el-tag size="small" :type="t.hooks ? 'success' : 'info'">
              {{ t.hooks ? '钩子:有(任务维度)' : '无钩子(只扩不缩)' }}
            </el-tag>
            <el-button v-if="t.hooks && t.partitions.length > t.min" size="small" text type="primary" @click="startDemo(t)">演示缩容 →</el-button>
          </div>
        </div>
        <div class="mini-note">
          钩子本身是<b>业务代码</b>(如 bucket_map 收缩),由任务提交 TaskSpec 时声明,后台只能<b>触发与观察进度</b>,不能编写;未声明钩子的任务(如索引器、计算面单元)在 ns 开关打开时也<b>只扩不缩</b>。
        </div>
      </div>

      <!-- 缩容演示 -->
      <div class="card">
        <h3 class="card-title">逻辑缩容流程演示(框架编排 + 业务钩子协作)</h3>
        <div v-if="!demo" class="empty">
          <p>Kafka 分区只增不减:缩容靠"生产端切路由 + 消费端追平回收"三步。</p>
          <p>在左侧选一个声明了钩子的任务点"演示缩容",观察三步编排。</p>
        </div>
        <el-steps v-else :active="demo.step" direction="vertical" style="height:auto">
          <el-step v-for="(s, i) in demoSteps" :key="i" :title="s.label" :description="s.desc" :status="s.active ? 'finish' : 'wait'" />
        </el-steps>
        <el-button v-if="demo" type="primary" style="margin-top:14px" @click="nextStep">
          {{ demo.step < 3 ? '下一步' : '完成缩容' }}
        </el-button>
      </div>
    </div>

    <!-- 事件历史 -->
    <div class="card" style="margin-top:16px">
      <h3 class="card-title">扩缩容与调度事件(ns 聚合视图)</h3>
      <el-table :data="store.scalingEvents" size="small">
        <el-table-column prop="time" label="时间" width="130" />
        <el-table-column label="ns" width="90">
          <template #default="{ row }"><span :style="{ color: nsColor[row.ns], fontWeight: 600 }">{{ row.ns }}</span></template>
        </el-table-column>
        <el-table-column prop="topic" label="任务" min-width="220" />
        <el-table-column prop="action" label="动作" min-width="220" />
        <el-table-column prop="detail" label="说明" min-width="260" />
        <el-table-column label="结果" width="90">
          <template #default="{ row }"><el-tag size="small" :type="row.result === '成功' ? 'success' : 'danger'">{{ row.result }}</el-tag></template>
        </el-table-column>
        <el-table-column label="触发" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.by === 'auto' ? 'info' : 'warning'">{{ row.by === 'auto' ? '自动' : '人工' }}</el-tag>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <div class="tip">
      <b>防抖动三件套</b>:阈值连续判定(扩容 3 周期/缩容 6 周期)+ 冷却期(10min,期间冻结决策)+ 渐进步长(每次最多翻倍/逐个回收)。人工强制豁免冷却;人工优先级 &gt; 自动。
    </div>
  </div>
</template>

<style scoped>
.page-title { font-size: 22px; font-weight: 700; color: #0f172a; }
.page-desc { color: #64748b; font-size: 13px; margin: 8px 0 18px; max-width: 960px; }
.sec { font-size: 14px; color: #334155; margin-bottom: 10px; }
.ns-rule-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-bottom: 16px; }
.ns-rule-card { background: #fff; border-radius: 12px; padding: 14px 16px; border-top: 4px solid; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.nr-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
.nr-row { display: flex; justify-content: space-between; font-size: 12.5px; margin-bottom: 7px; }
.nr-row span { color: #94a3b8; }
.nr-row b { color: #334155; }
.nr-switch { display: flex; justify-content: space-between; align-items: center; font-size: 12.5px; color: #94a3b8; margin-top: 10px; border-top: 1px dashed #e2e8f0; padding-top: 10px; }
.two-col { display: grid; grid-template-columns: 1.4fr 1fr; gap: 16px; }
.card { background: #fff; border-radius: 12px; padding: 18px 20px; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.card-title { font-size: 15px; color: #0f172a; margin-bottom: 12px; }
.rule-row { border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px 14px; margin-bottom: 10px; }
.rule-head { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
.ns-dot { width: 9px; height: 9px; border-radius: 50%; }
.rule-topic { font-size: 13px; }
.rule-ns { font-size: 11px; font-weight: 600; }
.unit-tag { font-size: 11px; background: #ecfdf5; color: #047857; border-radius: 4px; padding: 1px 6px; }
.rule-body { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; }
.rule-item { font-size: 12px; color: #64748b; }
.mini-note { font-size: 12px; color: #64748b; line-height: 1.8; background: #f8fafc; border-radius: 8px; padding: 8px 12px; }
.empty { color: #94a3b8; font-size: 13px; line-height: 2; }
.tip { margin-top: 16px; background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 10px; padding: 12px 16px; font-size: 12.5px; color: #1e3a8a; line-height: 1.8; }
</style>

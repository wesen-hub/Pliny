<script setup>
import { ref } from 'vue'
import { store, nsColor } from '../store'
import { ElMessage } from 'element-plus'

const dialog = ref(false)
const form = ref({ name: '', label: '', desc: '', minDef: 1, maxDef: 16 })
const weightDialog = ref(false)
const weightTarget = ref(null)
const weightVal = ref(1)

const statusMeta = { ONLINE: { label: '在线', type: 'success' }, DRAINING: { label: '迁空中', type: 'warning' }, SUSPECT: { label: '疑似离线', type: 'danger' } }

function now() {
  const d = new Date(); const p = n => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`
}

function instsOf(ns) { return store.instances.filter(i => i.ns === ns) }

function createNs() {
  if (!form.value.name || !form.value.label) return ElMessage.warning('请填写名称与描述')
  if (store.namespaces.some(n => n.name === form.value.name)) return ElMessage.warning('namespace 已存在')
  store.namespaces.push({
    name: form.value.name, label: form.value.label, desc: form.value.desc || '(新场景)',
    handlerType: '-', status: 'PROVISIONED', minDef: form.value.minDef, maxDef: form.value.maxDef,
    rps: 0, lag: 0, tasks: 0, partitions: 0,
    scaleRules: { up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldown: 10 }, scaleDownEnabled: false,
  })
  store.audits.unshift({ time: now(), user: 'admin', ns: form.value.name, action: '创建 namespace', detail: `名称 ${form.value.name},默认扩缩 [${form.value.minDef}, ${form.value.maxDef}]` })
  dialog.value = false
  form.value = { name: '', label: '', desc: '', minDef: 1, maxDef: 16 }
  ElMessage.success('namespace 已创建(空壳:实例与任务需部署/代码接入)')
}

function archive(ns) {
  store.audits.unshift({ time: now(), user: 'admin', ns: ns.name, action: '归档 namespace', detail: '禁止新任务,存量任务继续' })
  ElMessage.info('演示:归档操作已写入审计')
}

// 实例操作(在 ns 维度管理)
function drain(inst) {
  inst.status = 'DRAINING'
  store.audits.unshift({ time: now(), user: 'admin', ns: inst.ns, action: '实例下架', detail: `${inst.id} 进入 DRAINING,迁空分区后下线` })
  ElMessage.info('已进入 DRAINING:停止接新任务,存量分区由调度主迁移')
}
function pause(inst) {
  store.audits.unshift({ time: now(), user: 'admin', ns: inst.ns, action: '暂停接单', detail: `${inst.id} 不再接收新分区,存量照跑` })
  ElMessage.info('演示:已暂停接单')
}
function openWeight(inst) { weightTarget.value = inst; weightVal.value = inst.weight; weightDialog.value = true }
function saveWeight() {
  weightTarget.value.weight = weightVal.value
  store.audits.unshift({ time: now(), user: 'admin', ns: weightTarget.value.ns, action: '调权重', detail: `${weightTarget.value.id} 权重 → ${weightVal.value}` })
  weightDialog.value = false
  ElMessage.success('权重已调整(影响分配打分)')
}
</script>

<template>
  <div>
    <div class="head">
      <div>
        <h1 class="page-title">Namespace 管理</h1>
        <p class="page-desc">隔离域:独立选主、独立实例池、独立指标与规则,状态在 etcd 前缀 /dcp/{ns} 硬隔离。展开行可直接<b>管理该 ns 的实例</b>与<b>默认扩缩规则</b>。</p>
      </div>
      <el-button type="primary" @click="dialog = true">创建 namespace</el-button>
    </div>

    <el-table :data="store.namespaces" class="card-table">
      <el-table-column type="expand">
        <template #default="{ row }">
          <div class="expand-body">
            <el-tabs>
              <!-- 实例管理 -->
              <el-tab-pane :label="`实例管理(${instsOf(row.name).length})`">
                <el-table :data="instsOf(row.name)" size="small">
                  <el-table-column prop="id" label="实例" width="120"><template #default="{ row: i }"><b>{{ i.id }}</b></template></el-table-column>
                  <el-table-column prop="addr" label="地址" width="140" />
                  <el-table-column label="状态" width="100">
                    <template #default="{ row: i }"><el-tag size="small" :type="statusMeta[i.status].type">{{ statusMeta[i.status].label }}</el-tag></template>
                  </el-table-column>
                  <el-table-column label="权重" width="70"><template #default="{ row: i }">{{ i.weight.toFixed(1) }}</template></el-table-column>
                  <el-table-column label="持有分区" width="90"><template #default="{ row: i }">{{ i.partitions }}</template></el-table-column>
                  <el-table-column label="负载" min-width="160">
                    <template #default="{ row: i }"><el-progress :percentage="i.load" :color="i.load > 70 ? '#ef4444' : i.load > 50 ? '#f59e0b' : '#10b981'" /></template>
                  </el-table-column>
                  <el-table-column label="操作" width="220">
                    <template #default="{ row: i }">
                      <el-button size="small" @click="openWeight(i)">调权重</el-button>
                      <el-button size="small" @click="pause(i)">暂停接单</el-button>
                      <el-button size="small" type="danger" plain :disabled="i.status !== 'ONLINE'" @click="drain(i)">下架</el-button>
                    </template>
                  </el-table-column>
                </el-table>
                <div class="mini-note">实例归属 ns 是部署属性(启动配置声明),后台管池的进出与调配;新实例上线自动注册,可配置审批。</div>
              </el-tab-pane>

              <!-- 默认扩缩规则 -->
              <el-tab-pane label="默认扩缩规则">
                <div class="rule-grid">
                  <div class="rule-box">
                    <div class="r-t">并发边界(默认)</div>
                    <div class="r-v">[{{ row.minDef }}, {{ row.maxDef }}]</div>
                  </div>
                  <div class="rule-box">
                    <div class="r-t">扩容水位</div>
                    <div class="r-v">&gt;{{ Math.round(row.scaleRules.up * 100) }}% × {{ row.scaleRules.upPeriods }} 周期</div>
                  </div>
                  <div class="rule-box">
                    <div class="r-t">缩容水位</div>
                    <div class="r-v">&lt;{{ Math.round(row.scaleRules.down * 100) }}% × {{ row.scaleRules.downPeriods }} 周期</div>
                  </div>
                  <div class="rule-box">
                    <div class="r-t">冷却期</div>
                    <div class="r-v">{{ row.scaleRules.cooldown }} min</div>
                  </div>
                  <div class="rule-box">
                    <div class="r-t">缩容总开关(ns 级)</div>
                    <el-switch v-model="row.scaleDownEnabled" @change="v => { store.audits.unshift({ time: now(), user: 'admin', ns: row.name, action: '缩容开关', detail: `${row.name} 缩容 → ${v ? '开启' : '关闭'}` }); ElMessage.success(`演示:ns 缩容总开关 → ${v ? '开启' : '关闭'}`) }" />
                  </div>
                </div>
                <div class="mini-note">
                  规则维度:钩子 = <b>任务维度</b>(业务代码,任务提交时声明,本页不配置);规则 = <b>ns 默认 + 任务覆盖</b>;缩容开关 = ns 总开关 + 任务"只扩不缩"声明。未实现钩子的任务即使 ns 开关打开也只扩不缩。
                </div>
              </el-tab-pane>
            </el-tabs>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="namespace" width="130">
        <template #default="{ row }"><span :style="{ color: nsColor[row.name] || '#0f172a', fontWeight: 600 }">{{ row.name }}</span></template>
      </el-table-column>
      <el-table-column prop="label" label="场景" width="120" />
      <el-table-column prop="desc" label="描述" min-width="220" />
      <el-table-column label="逻辑装载" width="110">
        <template #default="{ row }"><el-tag size="small" :type="row.handlerType === 'spi' ? 'success' : row.handlerType === 'embedded' ? 'primary' : 'info'">{{ row.handlerType }}</el-tag></template>
      </el-table-column>
      <el-table-column label="实例 / 任务 / 分区" width="160">
        <template #default="{ row }">{{ instsOf(row.name).length }} / {{ row.tasks }} / {{ row.partitions }}</template>
      </el-table-column>
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <el-tag size="small" :type="row.status === 'RUNNING' ? 'success' : 'warning'">{{ row.status === 'RUNNING' ? '运行中' : '空壳(待部署)' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="90">
        <template #default="{ row }">
          <el-button size="small" :disabled="row.status !== 'RUNNING'" @click="archive(row)">归档</el-button>
        </template>
      </el-table-column>
    </el-table>

    <div class="tip">
      <b>边界说明</b>:后台创建 namespace 是"预配置"——写 /dcp/{ns}/ 前缀与默认规则。要让 ns 真正工作,还需要 ① 部署该 ns 的实例进程(启动配置声明 ns)② 业务外壳代码向该 ns 提交 TaskSpec。这两步是部署/开发动作,不是后台点出来的。
    </div>

    <el-dialog v-model="dialog" title="创建 namespace" width="480">
      <el-form label-width="110px">
        <el-form-item label="ns 名称" required><el-input v-model="form.name" placeholder="如:sink / archive / push" /></el-form-item>
        <el-form-item label="场景名称" required><el-input v-model="form.label" placeholder="如:归档消费" /></el-form-item>
        <el-form-item label="描述"><el-input v-model="form.desc" type="textarea" :rows="2" placeholder="消费场景说明" /></el-form-item>
        <el-form-item label="默认并发边界">
          <el-input-number v-model="form.minDef" :min="1" :max="64" /> ~ <el-input-number v-model="form.maxDef" :min="1" :max="1024" style="margin-left:8px" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialog = false">取消</el-button>
        <el-button type="primary" @click="createNs">创建(写 etcd 前缀)</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="weightDialog" title="调整实例权重" width="420">
      <p style="margin-bottom:12px;color:#475569;font-size:13px">权重参与分配打分:高配机器给大权重,分配时按 load/capacity 比率取最小。</p>
      <el-slider v-model="weightVal" :min="0.2" :max="3" :step="0.1" show-input />
      <template #footer>
        <el-button @click="weightDialog = false">取消</el-button>
        <el-button type="primary" @click="saveWeight">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.page-title { font-size: 22px; font-weight: 700; color: #0f172a; }
.page-desc { color: #64748b; font-size: 13px; margin: 8px 0 18px; max-width: 860px; }
.head { display: flex; justify-content: space-between; align-items: flex-start; }
.card-table { background: #fff; border-radius: 12px; padding: 8px; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.expand-body { padding: 6px 16px 16px 46px; }
.rule-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 12px; }
.rule-box { background: #f8fafc; border-radius: 10px; padding: 12px 14px; }
.r-t { font-size: 12px; color: #94a3b8; margin-bottom: 6px; }
.r-v { font-size: 14px; font-weight: 600; color: #0f172a; }
.mini-note { font-size: 12px; color: #64748b; margin-top: 10px; line-height: 1.8; background: #f8fafc; border-radius: 8px; padding: 8px 12px; }
.tip { margin-top: 16px; background: #fffbeb; border: 1px solid #fde68a; border-radius: 10px; padding: 12px 16px; font-size: 12.5px; color: #78350f; line-height: 1.7; }
</style>

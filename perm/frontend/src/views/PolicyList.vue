<template>
  <div class="page">
    <div class="page-header">
      <h2>策略管理</h2>
      <div class="sub">
        策略生命周期状态机（R-M1）：草稿 → 审批中 → 已发布 → 下线过渡 → 退役
        <el-tag size="small" type="info" effect="plain" style="margin-left: 6px">perm-admin :8082</el-tag>
        <el-button size="small" text type="primary" style="margin-left: 8px" @click="load" :loading="loading">刷新</el-button>
      </div>
    </div>

    <!-- 统计卡 -->
    <div class="stats">
      <div v-for="s in statCards" :key="s.label" class="stat-card">
        <div class="stat-num" :style="{ color: s.color }">{{ s.value }}</div>
        <div class="stat-label">{{ s.label }}</div>
      </div>
    </div>

    <div class="card">
      <div class="toolbar">
        <el-radio-group v-model="filter" size="small">
          <el-radio-button value="all">全部</el-radio-button>
          <el-radio-button value="ACTIVE">已发布</el-radio-button>
          <el-radio-button value="IN_REVIEW">审批中</el-radio-button>
          <el-radio-button value="DRAFT">草稿</el-radio-button>
        </el-radio-group>
        <div>
          <el-button type="primary" size="small" @click="goNew">+ 新建策略</el-button>
        </div>
      </div>

      <el-table :data="filteredPolicies" style="width: 100%" v-loading="loading">
        <el-table-column label="策略" min-width="200">
          <template #default="{ row }">
            <div class="pol-name">{{ row.policyName }}</div>
            <div class="pol-id mono">{{ row.policyId }} · 已发布 v{{ row.publishedVersion }}</div>
          </template>
        </el-table-column>
        <el-table-column label="效果" width="90">
          <template #default="{ row }">
            <el-tag size="small" :class="row.effect === 'GRANT' ? 'tag-effect-grant' : 'tag-effect-deny'" effect="plain">
              {{ row.effect }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="主体" width="170">
          <template #default="{ row }">
            <span class="mono">{{ row.subjectType }}:</span>
            <span class="mono">{{ (row.subjectIds || []).join(' / ') }}</span>
          </template>
        </el-table-column>
        <el-table-column label="资源" width="180">
          <template #default="{ row }">
            <span class="mono">{{ row.resourceId }}</span>
          </template>
        </el-table-column>
        <el-table-column label="动作" width="170">
          <template #default="{ row }">
            <span class="mono">{{ (row.actions || []).join(' · ') }}</span>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="statusMeta[row.status]?.type">{{ statusMeta[row.status]?.label || row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="创建人" width="90" prop="createdBy" />
        <el-table-column label="操作" width="250" fixed="right">
          <template #default="{ row }">
            <el-button size="small" text type="primary" @click="goEdit(row)">查看/编辑</el-button>
            <el-dropdown
              v-if="(transitions[row.status] || []).length"
              trigger="click"
              @command="(cmd) => doTransition(row, cmd)"
            >
              <el-button size="small" type="primary" plain>
                流转 <el-icon><ArrowDown /></el-icon>
              </el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item v-for="t in transitions[row.status]" :key="t.action" :command="t">
                    <span :style="{ color: t.type === 'danger' ? '#dc2626' : t.type === 'success' ? '#16a34a' : '#2563eb' }">{{ t.label }}</span>
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
            <span v-else class="no-op">—</span>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <!-- 冲突确认弹窗（approve 被 WARNING 拦截时） -->
    <el-dialog v-model="confirmDialog.visible" title="发布前冲突确认（WARNING）" width="640px">
      <div class="confirm-tip">
        策略 <b class="mono">{{ confirmDialog.policyId }}</b> 与存量策略存在冲突，需确认后强制放行（R-M8）。
      </div>
      <el-table :data="confirmDialog.conflicts" size="small" border>
        <el-table-column label="级别" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.severity === 'WARNING' ? 'warning' : 'info'">{{ row.severity }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="类型" width="160" prop="type" />
        <el-table-column label="详情" prop="detail" />
      </el-table>
      <template #footer>
        <el-button size="small" @click="confirmDialog.visible = false">取消</el-button>
        <el-button size="small" type="warning" @click="doForceApprove">确认放行</el-button>
      </template>
    </el-dialog>

    <!-- 状态机说明 -->
    <div class="card">
      <div class="card-title">策略生命周期状态机（R-M1）</div>
      <div class="fsm">
        <div v-for="(s, i) in fsmStates" :key="s.key" class="fsm-item">
          <div class="fsm-node">
            <el-tag size="small" :type="statusMeta[s.key]?.type">{{ statusMeta[s.key]?.label }}</el-tag>
            <div class="fsm-desc">{{ s.desc }}</div>
          </div>
          <div v-if="i < fsmStates.length - 1" class="fsm-arrow">→</div>
        </div>
      </div>
      <div class="fsm-note">
        approve 即触发原子发布六步（R-M3）：前置校验 → 冲突检测 → 锁定 → 持久化版本 → 事件广播 → 确认存档；
        PDP 侧轮询版本号热更新快照（当前快照版本见「全链路可视化」页）。
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { ArrowDown } from '@element-plus/icons-vue'
import { policyApi, backendStatusMeta, backendTransitions } from '../api/admin'

const router = useRouter()
const policies = ref([])
const loading = ref(false)
const filter = ref('all')
const statusMeta = backendStatusMeta
const transitions = backendTransitions

const filteredPolicies = computed(() => {
  if (filter.value === 'all') return policies.value
  return policies.value.filter(p => p.status === filter.value)
})

const statCards = computed(() => [
  { label: '策略总数', value: policies.value.length, color: '#2563eb' },
  { label: '已发布 ACTIVE', value: policies.value.filter(p => p.status === 'ACTIVE').length, color: '#16a34a' },
  { label: '审批中', value: policies.value.filter(p => p.status === 'IN_REVIEW').length, color: '#f59e0b' },
  { label: '草稿', value: policies.value.filter(p => p.status === 'DRAFT').length, color: '#7c3aed' }
])

const fsmStates = [
  { key: 'DRAFT', desc: '编辑中，未生效' },
  { key: 'IN_REVIEW', desc: '待审批，未生效' },
  { key: 'ACTIVE', desc: '参与判定（快照加载）' },
  { key: 'DEPRECATED', desc: '下线过渡期' },
  { key: 'RETIRED', desc: '终态，退出判定' }
]

const load = async () => {
  loading.value = true
  try {
    policies.value = await policyApi.list()
  } catch (e) {
    ElMessage.error(e.message)
  } finally {
    loading.value = false
  }
}

// WARNING 冲突确认弹窗状态
const confirmDialog = ref({ visible: false, policyId: '', conflicts: [] })

const doTransition = async (row, t) => {
  if (t.action === 'approve') {
    try {
      await policyApi.transition(row.policyId, 'approve')
      ElMessage.success(`「${row.policyName}」已审批通过并发布`)
      await load()
    } catch (e) {
      if (e.code === 'conflict_warning') {
        confirmDialog.value = { visible: true, policyId: row.policyId, conflicts: e.conflicts }
      } else if (e.code === 'conflict_blocked') {
        ElMessageBox.alert(
          (e.conflicts || []).map(c => `${c.severity} ${c.type}：${c.detail}`).join('\n'),
          '发布被阻断（ERROR 冲突）',
          { confirmButtonText: '知道了', type: 'error' }
        )
      } else {
        ElMessage.error(e.message)
      }
    }
    return
  }
  try {
    await policyApi.transition(row.policyId, t.action)
    ElMessage.success(`「${row.policyName}」已流转为【${statusMeta[t.to].label}】`)
    await load()
  } catch (e) {
    ElMessage.error(e.message)
  }
}

const doForceApprove = async () => {
  const id = confirmDialog.value.policyId
  confirmDialog.value.visible = false
  try {
    const pub = await policyApi.transition(id, 'approve', { force: true })
    ElMessage.success(`已强制放行发布，planVersion → v${pub.version}`)
    await load()
  } catch (e) {
    ElMessage.error(e.message)
  }
}

const goEdit = (row) => router.push(`/policies/${row.policyId}/edit`)
const goNew = () => router.push('/policies/new')

onMounted(load)
</script>

<style scoped>
.stats { display: flex; gap: 12px; margin-bottom: 16px; }
.stat-card {
  flex: 1;
  background: #fff;
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  padding: 14px 16px;
}
.stat-num { font-size: 24px; font-weight: 700; }
.stat-label { font-size: 12px; color: var(--gray-500); margin-top: 2px; }
.toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
.pol-name { font-size: 13px; font-weight: 600; color: var(--gray-800); }
.pol-id { font-size: 11px; color: var(--gray-400); margin-top: 2px; }
.no-op { color: var(--gray-300); font-size: 12px; }
.confirm-tip { font-size: 13px; margin-bottom: 10px; color: var(--gray-700); }

.fsm { display: flex; align-items: flex-start; gap: 8px; flex-wrap: wrap; }
.fsm-item { display: flex; align-items: flex-start; }
.fsm-node { text-align: center; padding: 8px 12px; border-radius: 8px; border: 1px solid var(--gray-200); min-width: 90px; }
.fsm-desc { font-size: 11px; color: var(--gray-500); margin-top: 6px; }
.fsm-arrow { color: var(--gray-400); font-size: 18px; padding: 14px 4px 0; }
.fsm-note { margin-top: 14px; font-size: 12px; color: var(--gray-500); background: var(--primary-light); padding: 10px 12px; border-radius: 6px; }
</style>

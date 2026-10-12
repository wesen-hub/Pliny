<template>
  <div class="page">
    <div class="page-header">
      <h2>判定试验台</h2>
      <div class="sub">
        真实调用 PDP 判定链（perm-pdp :8081）：编辑请求 → DecisionEngine 七步判定 → 方言渲染（模板无值 C1）→ HMAC 签名产物
      </div>
    </div>

    <!-- 场景预置 -->
    <div class="card">
      <div class="card-title">演示场景（点击载入，可再编辑）</div>
      <div class="scenario-grid">
        <div
          v-for="s in scenarios"
          :key="s.id"
          class="scenario-item"
          :class="{ active: activeScenario === s.id }"
          @click="loadScenario(s)"
        >
          <div class="sc-name">{{ s.name }}</div>
          <el-tag size="small" :type="s.badgeType" effect="plain">{{ s.badge }}</el-tag>
          <div class="sc-highlights">
            <span v-for="h in s.highlights" :key="h" class="sc-hl">{{ h }}</span>
          </div>
        </div>
      </div>
    </div>

    <div class="pg-layout">
      <!-- 左：请求 -->
      <div class="pg-left">
        <div class="card">
          <div class="card-title">① AuthorizationRequest（PEP → PDP）</div>
          <el-form label-width="100px" size="small" label-position="left">
            <el-form-item label="userId"><el-input v-model="req.userId" class="mono" /></el-form-item>
            <el-form-item label="roles">
              <el-select v-model="req.roles" multiple filterable allow-create default-first-option style="width: 100%">
                <el-option v-for="r in roleCandidates" :key="r" :value="r" :label="r" />
              </el-select>
            </el-form-item>
            <el-form-item label="attributes">
              <el-input v-model="req.attributesText" type="textarea" :rows="3" class="mono" placeholder='{"region":"华东","allowedStatus":["PAID","SHIPPED"]}' />
            </el-form-item>
            <el-form-item label="tagLevels">
              <el-input v-model="req.tagLevelsText" class="mono" placeholder='{"业务域/客户价值/高价值": 2}' />
            </el-form-item>
            <el-form-item label="resourceId"><el-input v-model="req.resourceId" class="mono" /></el-form-item>
            <el-form-item label="action"><el-input v-model="req.action" class="mono" /></el-form-item>
            <el-form-item label="dialect"><el-input v-model="req.dialect" class="mono" /></el-form-item>
          </el-form>
          <el-button type="primary" size="small" @click="doAuthorize" :loading="judging">▶ 发送判定请求</el-button>
          <div v-if="judgeError" class="judge-error">{{ judgeError }}</div>
        </div>
      </div>

      <!-- 右：真实产物 -->
      <div class="pg-right">
        <div class="card">
          <div class="card-title">② 判定结果（DecisionEngine）</div>
          <div v-if="artifact" class="decision-banner" :class="artifact.decision === 'PERMIT' ? 'is-permit' : 'is-deny'">
            <div class="decision-word">{{ artifact.decision }}</div>
            <div class="decision-reason">{{ artifact.denyReason || '—' }}</div>
          </div>
          <div v-else class="decision-banner is-idle"><div class="decision-word">待判定</div></div>
          <div v-if="artifact" class="matched">
            <span class="k">命中的策略：</span>
            <el-tag v-for="m in artifact.matchedPolicies" :key="m" size="small" type="info" effect="plain" style="margin-right: 6px">{{ m }}</el-tag>
            <span v-if="!artifact.matchedPolicies?.length" style="color: var(--gray-400)">无（fail-closed 兜底 R-F1）</span>
          </div>
        </div>

        <div class="card" v-if="artifact">
          <div class="card-title">③ 判定产物（签名 artifact，模板无值 C1）</div>
          <el-tabs size="small">
            <el-tab-pane label="行谓词模板">
              <pre class="mono artifact-view">{{ artifact.rowPredicateTemplate || '（无行谓词）' }}</pre>
              <div v-if="artifact.placeholders?.length" class="ph-list">
                <span class="k">占位符：</span>
                <el-tag v-for="p in artifact.placeholders" :key="p.name" size="small" effect="plain" style="margin-right: 6px">
                  {{ p.name }} ← {{ p.ref }} <template v-if="p.value !== null && p.value !== undefined">= {{ p.value }}</template>
                </el-tag>
                <div class="hint">模板无值：值在 PEP 绑定阶段注入（BindException → DENY）</div>
              </div>
            </el-tab-pane>
            <el-tab-pane label="列与掩码">
              <div class="kv">
                <div class="kv-item"><span class="k">可见列（列裁剪 R-D8）</span><span class="v mono">{{ artifact.visibleColumns ? artifact.visibleColumns.join(', ') : '不裁剪' }}</span></div>
                <div class="kv-item"><span class="k">掩码（最严者胜 R-D9）</span><span class="v mono">{{ artifact.masks?.length ? artifact.masks.map(m => `${m.column}→${m.type}`).join(', ') : '无' }}</span></div>
              </div>
            </el-tab-pane>
            <el-tab-pane label="标签与义务">
              <div class="kv">
                <div class="kv-item"><span class="k">tagScope</span><span class="v mono">{{ artifact.tagScope ? `${artifact.tagScope.tagPath} [${artifact.tagScope.expandMode}] → ${artifact.tagScope.targetKey} IN (${(artifact.tagScope.entities || []).join(', ')})` : '无' }}</span></div>
                <div class="kv-item"><span class="k">义务 obligations</span><span class="v mono">{{ artifact.obligations?.length ? artifact.obligations.map(o => `${o.type}:${o.mode}`).join(', ') : '无' }}</span></div>
                <div class="kv-item"><span class="k">planVersion</span><span class="v mono">{{ artifact.planVersion }}</span></div>
              </div>
            </el-tab-pane>
            <el-tab-pane label="签名">
              <pre class="mono artifact-view">{{ artifact.signature ? artifact.signature.slice(0, 48) + '…' : '无' }}</pre>
              <div class="hint">HMAC-SHA256（R-Sec1）；升级路径：PDP 私钥签名 + keyId（R-Sec6）</div>
            </el-tab-pane>
          </el-tabs>
        </div>

        <div class="card" v-if="artifact">
          <div class="card-title">④ 原始 JSON 响应</div>
          <pre class="mono artifact-view">{{ pretty(artifact) }}</pre>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import { pdpApi } from '../api/pdp'

const roleCandidates = ['regional_manager', 'marketing_analyst', 'intern', 'employee', 'data_analyst']

// 场景 = 内置快照 4 条策略对应的请求 + fail-closed
const scenarios = [
  {
    id: 's1',
    name: '区域经理 · 行级过滤',
    badge: '行谓词+掩码+裁剪',
    badgeType: 'success',
    highlights: ['pol_region_001', 'region 下推', 'amount HASH'],
    request: {
      userId: 'lisi', roles: ['regional_manager'],
      attributes: { region: '华东', allowedStatus: ['PAID', 'SHIPPED'] },
      tagLevels: {}, resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql'
    }
  },
  {
    id: 's2',
    name: '张三 · DENY-overrides',
    badge: 'R-D2',
    badgeType: 'danger',
    highlights: ['pol_deny_zhangsan_002', 'DENY 压制 GRANT'],
    request: {
      userId: 'zhangsan', roles: ['regional_manager'],
      attributes: { region: '华东', allowedStatus: ['PAID'] },
      tagLevels: {}, resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql'
    }
  },
  {
    id: 's3',
    name: '营销分析 · 标签模式 A',
    badge: 'tagScope 列表下推',
    badgeType: 'primary',
    highlights: ['pol_marketing_tag_003', 'customer_id IN 下推'],
    request: {
      userId: 'wangwu', roles: ['marketing_analyst'],
      attributes: {}, tagLevels: { '业务域/客户价值/高价值': 2 },
      resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql'
    }
  },
  {
    id: 's4',
    name: '实习生 · 表级拒绝',
    badge: 'HIDDEN',
    badgeType: 'warning',
    highlights: ['pol_intern_hidden_004', 'R-D6 表级短路'],
    request: {
      userId: 'zhaoliu', roles: ['intern'],
      attributes: {}, tagLevels: {}, resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql'
    }
  },
  {
    id: 's5',
    name: '未知用户 · 零权限',
    badge: 'fail-closed',
    badgeType: 'danger',
    highlights: ['R-F1 兜底 DENY'],
    request: {
      userId: 'stranger', roles: [],
      attributes: {}, tagLevels: {}, resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql'
    }
  }
]

const req = ref(initReq(scenarios[0].request))
const activeScenario = ref('s1')
const artifact = ref(null)
const judging = ref(false)
const judgeError = ref('')

function initReq(r) {
  return {
    userId: r.userId,
    roles: [...r.roles],
    attributesText: JSON.stringify(r.attributes),
    tagLevelsText: JSON.stringify(r.tagLevels),
    resourceId: r.resourceId,
    action: r.action,
    dialect: r.dialect
  }
}

const loadScenario = (s) => {
  activeScenario.value = s.id
  req.value = initReq(s.request)
  artifact.value = null
  judgeError.value = ''
}

const parseJson = (text, label) => {
  if (!text || !text.trim()) return {}
  try { return JSON.parse(text) } catch { throw new Error(`${label} 不是合法 JSON`) }
}

const doAuthorize = async () => {
  judging.value = true
  judgeError.value = ''
  try {
    artifact.value = await pdpApi.authorize({
      userId: req.value.userId,
      roles: req.value.roles,
      attributes: parseJson(req.value.attributesText, 'attributes'),
      tagLevels: parseJson(req.value.tagLevelsText, 'tagLevels'),
      resourceId: req.value.resourceId,
      action: req.value.action,
      dialect: req.value.dialect
    })
    ElMessage.success(`判定完成：${artifact.value.decision}`)
  } catch (e) {
    judgeError.value = e.message
    artifact.value = null
  } finally {
    judging.value = false
  }
}

const pretty = (o) => JSON.stringify(o, null, 2)
</script>

<style scoped>
.scenario-grid { display: flex; gap: 10px; flex-wrap: wrap; }
.scenario-item {
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  padding: 10px 12px;
  cursor: pointer;
  min-width: 190px;
  transition: all .15s;
}
.scenario-item:hover { border-color: var(--primary); }
.scenario-item.active { border-color: var(--primary); box-shadow: 0 0 0 3px var(--primary-light); }
.sc-name { font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.sc-highlights { margin-top: 6px; display: flex; gap: 6px; flex-wrap: wrap; }
.sc-hl { font-size: 10px; color: var(--gray-500); background: var(--gray-50); border-radius: 4px; padding: 1px 6px; }

.pg-layout { display: flex; gap: 16px; align-items: flex-start; margin-top: 16px; }
.pg-left { width: 400px; flex-shrink: 0; position: sticky; top: 20px; }
.pg-right { flex: 1; min-width: 0; }
.kv .kv-item { display: flex; justify-content: space-between; gap: 12px; padding: 6px 0; border-bottom: 1px dashed var(--gray-100); font-size: 12px; }
.kv .k { color: var(--gray-500); flex-shrink: 0; }
.kv .v { text-align: right; word-break: break-all; }
.matched { margin-top: 10px; font-size: 12px; }
.matched .k { color: var(--gray-500); }
.ph-list { margin-top: 8px; font-size: 12px; }
.ph-list .k { color: var(--gray-500); }
.hint { font-size: 11px; color: var(--gray-400); margin-top: 6px; }
.judge-error { margin-top: 10px; font-size: 12px; color: #dc2626; background: #fee2e2; border-radius: 6px; padding: 8px 10px; }

.decision-banner { border-radius: 8px; padding: 16px; margin-bottom: 4px; }
.decision-banner.is-permit { background: #ecfdf5; border: 1px solid #a7f3d0; }
.decision-banner.is-deny { background: #fef2f2; border: 1px solid #fecaca; }
.decision-banner.is-idle { background: var(--gray-50); border: 1px solid var(--gray-200); }
.decision-word { font-size: 26px; font-weight: 800; letter-spacing: 2px; }
.is-permit .decision-word { color: #16a34a; }
.is-deny .decision-word { color: #dc2626; }
.is-idle .decision-word { color: var(--gray-400); }
.decision-reason { font-size: 12px; color: var(--gray-600); margin-top: 6px; }

.artifact-view {
  background: #0f172a;
  color: #a5f3fc;
  border-radius: 8px;
  padding: 12px;
  font-size: 11px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 320px;
  overflow: auto;
}
</style>

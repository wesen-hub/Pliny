<template>
  <div class="page">
    <div class="page-header editor-header">
      <div>
        <el-button size="small" @click="$router.push('/policies')">← 返回列表</el-button>
      </div>
      <div style="flex: 1">
        <h2>策略编辑器 · 管理面 DSL</h2>
        <div class="sub">与 perm-common PolicyDsl 字段一一对应；左侧表单、右侧实时 JSON；仅 DRAFT 状态可编辑保存</div>
      </div>
      <div class="header-actions">
        <el-tag v-if="!isNew" size="small" :type="statusMeta[policy?.status]?.type">{{ statusMeta[policy?.status]?.label || policy?.status }}</el-tag>
        <el-tag v-else size="small" type="info">新建草稿</el-tag>
        <el-button type="primary" size="small" @click="doSave" :disabled="!editable" :loading="saving">保存策略</el-button>
        <el-button size="small" @click="resetForm">重置</el-button>
      </div>
    </div>

    <div class="editor-layout">
      <!-- 左：DSL 表单 -->
      <div class="form-col">
        <div class="card">
          <div class="card-title"><span class="part-no">①</span> 基本信息</div>
          <el-form label-width="90px" size="small" label-position="left">
            <el-form-item label="策略 ID">
              <el-input v-model="form.policyId" class="mono" :disabled="!isNew" />
            </el-form-item>
            <el-form-item label="策略名称"><el-input v-model="form.policyName" /></el-form-item>
            <el-form-item label="priority">
              <el-input-number v-model="form.priority" :min="0" :max="1000" size="small" />
              <span class="hint" style="margin-left: 8px">不参与裁决（C3），仅作展示</span>
            </el-form-item>
          </el-form>
        </div>

        <div class="card">
          <div class="card-title"><span class="part-no">②</span> 效果 effect</div>
          <el-form label-width="90px" size="small" label-position="left">
            <el-form-item label="effect">
              <el-radio-group v-model="form.effect">
                <el-radio-button value="GRANT">GRANT 允许</el-radio-button>
                <el-radio-button value="DENY">DENY 拒绝</el-radio-button>
              </el-radio-group>
              <div class="hint">DENY-overrides：DENY 无条件压制 GRANT（R-D2）</div>
            </el-form-item>
          </el-form>
        </div>

        <div class="card">
          <div class="card-title"><span class="part-no">③</span> 主体 subject</div>
          <el-form label-width="90px" size="small" label-position="left">
            <el-form-item label="subjectType">
              <el-radio-group v-model="form.subjectType">
                <el-radio-button value="role">role 角色</el-radio-button>
                <el-radio-button value="user">user 用户</el-radio-button>
                <el-radio-button value="group">group 团队</el-radio-button>
              </el-radio-group>
            </el-form-item>
            <el-form-item label="subjectIds">
              <el-select v-model="form.subjectIds" multiple filterable allow-create default-first-option style="width: 100%" :placeholder="subjectPlaceholder">
                <el-option v-for="i in subjectCandidates" :key="i" :value="i" :label="i" />
              </el-select>
              <div class="hint">ids 之间 OR 命中（集合相交）</div>
            </el-form-item>
          </el-form>
        </div>

        <div class="card">
          <div class="card-title"><span class="part-no">④</span> 资源与动作</div>
          <el-form label-width="90px" size="small" label-position="left">
            <el-form-item label="resourceId">
              <el-input v-model="form.resourceId" class="mono" placeholder="ns.db.table 三段式" />
            </el-form-item>
            <el-form-item label="actions">
              <el-select v-model="form.actions" multiple filterable allow-create default-first-option style="width: 100%" placeholder="select / read.* / tag.claim ...">
                <el-option v-for="a in actionCandidates" :key="a" :value="a" :label="a" />
              </el-select>
              <div class="hint">read.* 段内通配；tag.* 动作推导 tagLevel（read/count→T1+、claim→T2+、drill→T3+）</div>
            </el-form-item>
          </el-form>
        </div>

        <div class="card">
          <div class="card-title"><span class="part-no">⑤</span> 约束 constraints</div>
          <el-form label-width="90px" size="small" label-position="left">
            <el-form-item label="列可见性">
              <el-radio-group v-model="form.constraints.visible">
                <el-radio-button value="VISIBLE">VISIBLE</el-radio-button>
                <el-radio-button value="HIDDEN">HIDDEN</el-radio-button>
                <el-radio-button value="CONDITIONAL">CONDITIONAL</el-radio-button>
              </el-radio-group>
              <div class="hint">HIDDEN 表级拒绝（R-D6）；CONDITIONAL 需行谓词</div>
            </el-form-item>
            <el-form-item label="列白名单">
              <el-select v-model="form.constraints.columns" multiple filterable allow-create default-first-option style="width: 100%" placeholder="投影列（空 = 全部）">
                <el-option v-for="c in schemaColumns" :key="c" :value="c" :label="c" />
              </el-select>
            </el-form-item>
            <el-form-item label="行级规则">
              <div v-for="(r, idx) in form.constraints.rowRules" :key="idx" class="attr-row">
                <el-select v-model="r.column" style="width: 140px" placeholder="列">
                  <el-option v-for="c in schemaColumns" :key="c" :value="c" :label="c" />
                </el-select>
                <el-select v-model="r.op" style="width: 80px">
                  <el-option value="EQ" label="=" />
                  <el-option value="IN" label="IN" />
                </el-select>
                <el-select v-model="r.ref" style="width: 170px" clearable placeholder="ref（主体属性）">
                  <el-option value="subject.region" label="subject.region" />
                  <el-option value="subject.allowedStatus" label="subject.allowedStatus" />
                  <el-option value="subject.depts" label="subject.depts" />
                  <el-option value="subject.groups" label="subject.groups" />
                </el-select>
                <el-input v-model="r.value" style="flex: 1" :placeholder="r.ref ? 'ref 优先，此值忽略' : '字面量（IN 用逗号分隔）'" />
                <el-button text type="danger" size="small" @click="form.constraints.rowRules.splice(idx, 1)">删除</el-button>
              </div>
              <el-button size="small" text type="primary" @click="form.constraints.rowRules.push({ column: '', op: 'EQ', ref: '', value: '' })">+ 添加行规则</el-button>
              <div class="hint">ref 与 value 二选一；多条 AND 合并（R-D7）</div>
            </el-form-item>
            <el-form-item label="列掩码">
              <div v-for="(m, idx) in form.constraints.masks" :key="idx" class="attr-row">
                <el-select v-model="m.column" style="width: 150px" placeholder="列">
                  <el-option v-for="c in schemaColumns" :key="c" :value="c" :label="c" />
                </el-select>
                <el-select v-model="m.type" style="width: 110px">
                  <el-option v-for="t in maskCandidates" :key="t" :value="t" :label="t" />
                </el-select>
                <el-button text type="danger" size="small" @click="form.constraints.masks.splice(idx, 1)">删除</el-button>
              </div>
              <el-button size="small" text type="primary" @click="form.constraints.masks.push({ column: '', type: 'HASH' })">+ 添加掩码</el-button>
              <div class="hint">严格序：FULL > NULL > HASH > PARTIAL > REPLACE（R-D9）</div>
            </el-form-item>
            <el-form-item label="标签作用域">
              <el-switch v-model="hasTagScope" active-text="启用 tagScope（标签策略）" />
              <div v-if="hasTagScope" class="tag-scope-box">
                <el-input v-model="form.constraints.tagScope.tagPath" placeholder="标签路径，如 业务域/客户价值/高价值" class="mono" style="margin-bottom: 6px" />
                <div class="attr-row">
                  <el-select v-model="form.constraints.tagScope.expandMode" style="width: 110px">
                    <el-option value="LIST" label="LIST 枚举" />
                    <el-option value="SNAPSHOT" label="SNAPSHOT 快照" />
                  </el-select>
                  <el-input v-model="form.constraints.tagScope.targetKey" placeholder="目标键（如 customer_id）" style="width: 170px" />
                  <el-input-number v-model="form.constraints.tagScope.requiredLevel" :min="1" :max="3" size="small" />
                </div>
                <div class="hint">requiredLevel：圈选门槛（tag.claim T2+ / tag.drill T3+，R-T2）</div>
              </div>
            </el-form-item>
          </el-form>
        </div>

        <div class="card">
          <div class="card-title"><span class="part-no">⑥</span> 生效期</div>
          <el-form label-width="90px" size="small" label-position="left">
            <el-form-item label="生效起">
              <el-date-picker v-model="form.effectiveFrom" type="datetime" placeholder="生效开始" style="width: 240px" value-format="YYYY-MM-DDTHH:mm:ssZ" />
            </el-form-item>
            <el-form-item label="生效止">
              <el-date-picker v-model="form.effectiveTo" type="datetime" placeholder="留空 = 永久" style="width: 240px" value-format="YYYY-MM-DDTHH:mm:ssZ" />
            </el-form-item>
          </el-form>
        </div>

        <div class="card">
          <div class="card-title"><span class="part-no">⑦</span> 义务 obligations</div>
          <div v-for="(o, idx) in form.obligations" :key="idx" class="attr-row">
            <el-select v-model="o.type" style="width: 130px">
              <el-option value="AUDIT" label="AUDIT 审计" />
            </el-select>
            <el-radio-group v-model="o.mode" size="small">
              <el-radio-button value="sync">同步</el-radio-button>
              <el-radio-button value="async">异步</el-radio-button>
            </el-radio-group>
            <el-button text type="danger" size="small" @click="form.obligations.splice(idx, 1)">删除</el-button>
          </div>
          <el-button size="small" text type="primary" @click="form.obligations.push({ type: 'AUDIT', mode: 'sync' })">+ 添加义务</el-button>
        </div>
      </div>

      <!-- 右：JSON 实时预览 -->
      <div class="json-col">
        <div class="card json-card">
          <div class="card-title">
            PolicyDsl JSON（实时）
            <el-button size="small" text type="primary" @click="copyJson">复制</el-button>
          </div>
          <pre class="mono json-view">{{ jsonPreview }}</pre>
        </div>
        <div class="card json-card">
          <div class="card-title">编译与发布链路</div>
          <div class="cache-deduce">
            <div class="cd-row"><span class="k">编译</span><span class="v mono">PolicyCompiler：DSL → 策略 AST</span></div>
            <div class="cd-row"><span class="k">发布前</span><span class="v mono">冲突检测九类 C1~C9（R-M7/R-M8）</span></div>
            <div class="cd-row"><span class="k">发布后</span><span class="v mono">planVersion +1 → PDP 轮询热更新</span></div>
            <div class="cd-note">仅 ACTIVE 状态策略被 PDP 快照加载并参与判定。</div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { policyApi, backendStatusMeta } from '../api/admin'

const route = useRoute()
const router = useRouter()
const policyId = route.params.id
const isNew = route.path === '/policies/new'

const subjectCandidates = ['regional_manager', 'data_analyst', 'risk_analyst', 'intern', 'employee', 'team_east', 'team_data']
const schemaColumns = ['order_id', 'customer_id', 'amount', 'region', 'status', 'customer_phone', 'customer_id_card', 'domain', 'created_at']
const actionCandidates = ['select', 'read.*', 'export.csv', 'export.*', 'tag.read', 'tag.count', 'tag.claim', 'tag.drill']
const maskCandidates = ['FULL', 'NULL', 'HASH', 'PARTIAL', 'REPLACE']
const statusMeta = backendStatusMeta

const blankDsl = () => ({
  policyId: '',
  policyName: '未命名策略',
  effect: 'GRANT',
  subjectType: 'role',
  subjectIds: [],
  resourceId: '',
  actions: [],
  priority: 100,
  effectiveFrom: null,
  effectiveTo: null,
  constraints: {
    visible: 'CONDITIONAL',
    columns: [],
    masks: [],
    rowRules: [],
    tagScope: null
  },
  obligations: []
})

const form = ref(blankDsl())
const policy = ref(null)
const saving = ref(false)
const hasTagScope = ref(false)

/** 后端仅 DRAFT 可编辑（新建恒可编辑） */
const editable = computed(() => isNew || (policy.value && policy.value.status === 'DRAFT'))
const subjectPlaceholder = computed(() =>
  form.value.subjectType === 'role' ? '角色集合' : form.value.subjectType === 'user' ? '用户 ID 集合' : '团队集合')

/** masks 表单用 [{column,type}] 编辑，JSON 预览转 Map 结构 */
const jsonPreview = computed(() => {
  const dsl = JSON.parse(JSON.stringify(form.value))
  dsl.constraints.masks = Object.fromEntries(
    (form.value.constraints.masks || []).filter(m => m.column).map(m => [m.column, m.type]))
  if (!hasTagScope.value) dsl.constraints.tagScope = null
  else dsl.constraints.tagScope = { ...(form.value.constraints.tagScope || {}) }
  return JSON.stringify(dsl, null, 2)
})

const toRequestDsl = () => JSON.parse(jsonPreview.value)

const loadPolicy = async () => {
  if (isNew) return
  try {
    const p = await policyApi.get(policyId)
    policy.value = p
    // 响应 → 表单结构（masks Map → 列表）
    const c = p.constraints || {}
    form.value = {
      policyId: p.policyId,
      policyName: p.policyName,
      effect: p.effect,
      subjectType: p.subjectType,
      subjectIds: p.subjectIds || [],
      resourceId: p.resourceId,
      actions: p.actions || [],
      priority: p.priority ?? 100,
      effectiveFrom: p.effectiveFrom || null,
      effectiveTo: p.effectiveTo || null,
      constraints: {
        visible: c.visible || 'CONDITIONAL',
        columns: c.columns || [],
        masks: Object.entries(c.masks || {}).map(([column, type]) => ({ column, type })),
        rowRules: (c.rowRules || []).map(r => ({ column: r.column, op: r.op, ref: r.ref || '', value: r.value ?? '' })),
        tagScope: c.tagScope || { tagPath: '', expandMode: 'LIST', targetKey: '', requiredLevel: 2, entities: [] }
      },
      obligations: p.obligations || []
    }
    hasTagScope.value = !!c.tagScope
  } catch (e) {
    ElMessage.error(e.message)
    router.push('/policies')
  }
}

const doSave = async () => {
  if (!form.value.policyId || !form.value.policyName) {
    ElMessage.warning('请填写策略 ID 和名称')
    return
  }
  saving.value = true
  try {
    if (isNew) {
      await policyApi.create(toRequestDsl())
      ElMessage.success(`已创建「${form.value.policyName}」（草稿），可提交审批`)
    } else {
      await policyApi.update(policyId, toRequestDsl())
      ElMessage.success(`已保存「${form.value.policyName}」`)
    }
    router.push('/policies')
  } catch (e) {
    ElMessage.error(e.message)
  } finally {
    saving.value = false
  }
}

const resetForm = () => {
  if (isNew) form.value = blankDsl()
  else loadPolicy()
  ElMessage.info('已重置')
}

const copyJson = () => {
  navigator.clipboard?.writeText(jsonPreview.value)
  ElMessage.success('JSON 已复制')
}

onMounted(loadPolicy)
</script>

<style scoped>
.editor-header { display: flex; align-items: center; gap: 16px; }
.header-actions { display: flex; gap: 8px; align-items: center; }
.editor-layout { display: flex; gap: 16px; align-items: flex-start; }
.form-col { flex: 1; min-width: 0; }
.json-col { width: 420px; flex-shrink: 0; position: sticky; top: 20px; }
.json-card { margin-top: 0; }
.json-card + .json-card { margin-top: 16px; }
.json-view {
  background: #0f172a;
  color: #a5f3fc;
  border-radius: 8px;
  padding: 14px;
  max-height: 560px;
  overflow: auto;
  font-size: 11px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-all;
}
.part-no {
  display: inline-flex;
  width: 22px; height: 22px;
  border-radius: 50%;
  background: var(--primary);
  color: #fff;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  margin-right: 2px;
}
.hint { font-size: 11px; color: var(--gray-400); line-height: 1.5; margin-top: 2px; width: 100%; }
.attr-row { display: flex; gap: 8px; align-items: center; width: 100%; margin-bottom: 6px; }
.tag-scope-box { width: 100%; border: 1px dashed var(--gray-200); border-radius: 6px; padding: 8px; margin-top: 6px; }
.cache-deduce .cd-row { display: flex; justify-content: space-between; gap: 12px; padding: 8px 0; border-bottom: 1px dashed var(--gray-200); font-size: 12px; }
.cache-deduce .cd-row .k { color: var(--gray-500); flex-shrink: 0; }
.cache-deduce .cd-row .v { text-align: right; word-break: break-all; }
.cd-note { margin-top: 10px; font-size: 11px; color: var(--amber); background: var(--amber-light); padding: 8px 10px; border-radius: 6px; }
</style>

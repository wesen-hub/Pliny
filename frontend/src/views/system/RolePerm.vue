<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="head">
        <div>
          <div class="pg-title">系统权限管理<el-tag size="small" effect="plain" style="margin-left:8px">角色 × 权限点(已接后端)</el-tag></div>
          <el-text size="small" type="info">RBAC: 角色 × 权限点(菜单页 + 高危操作)。只管"能做什么";数据可见范围在「数据权限」页 —— 判定: <b>有操作权 ∧ 数据在可见范围</b> 才放行。</el-text>
        </div>
      </div>

      <div class="flex" style="gap:12px;margin-top:12px;align-items:flex-start">
        <!-- 左: 角色列表 -->
        <el-card shadow="never" class="side">
          <template #header>
            <div class="flex-between">
              <b>角色</b>
              <el-button size="small" text type="primary" @click="openCreate">＋ 新建</el-button>
            </div>
          </template>
          <div v-for="r in roles" :key="r.code" class="rrow" :class="{ on: cur === r.code }" @click="select(r)">
            <div class="rname">
              <b>{{ r.name }}</b>
              <el-tag v-if="isBuiltin(r.code)" size="small" effect="plain" type="info">预置</el-tag>
              <el-tag v-else size="small" effect="plain" type="warning">自定义</el-tag>
            </div>
            <div class="dim">{{ r.desc || '—' }}</div>
          </div>
        </el-card>

        <!-- 右: 角色详情 + 权限树 -->
        <div style="flex:1;min-width:0">
          <el-card shadow="never" v-loading="loading">
            <template #header>
              <div class="flex-between">
                <div>
                  <b>{{ curRole.name || '—' }}</b>
                  <el-tag size="small" effect="plain" style="margin-left:6px" :type="isBuiltin(curRole.code) ? 'info' : 'warning'">{{ isBuiltin(curRole.code) ? '预置角色' : '自定义角色' }}</el-tag>
                  <el-text size="small" type="info" style="margin-left:8px">code: {{ curRole.code }}<span v-if="curRole.desc"> · {{ curRole.desc }}</span></el-text>
                </div>
                <div>
                  <el-button v-if="curRole.code && !isBuiltin(curRole.code)" size="small" text type="danger" :loading="deleting" @click="delRole">删除角色</el-button>
                  <el-button size="small" type="primary" :loading="saving" @click="save">{{ saved ? '已保存 ✓' : '保存权限' }}</el-button>
                </div>
              </div>
            </template>

            <div class="perm-tip">
              权限树 = 菜单页(一级, 控制导航) + 操作(二级 action, 控制按钮/接口)。<b>高危点</b>(发布任务/启停流/删除/导出/管理类)单独授权。ADMIN 拥有全部权限, 无需配置。数据读取另受数据权限约束。
            </div>

            <el-tree ref="treeRef" :data="tree" show-checkbox node-key="id" default-expand-all
              :props="{ label: 'label', children: 'children' }">
              <template #default="{ data }">
                <span class="pnode">
                  <el-tag v-if="data.type === 'menu'" size="small" effect="plain" type="primary">{{ data.module }}</el-tag>
                  <el-tag v-else size="small" effect="plain" type="warning">动作</el-tag>
                  {{ data.label }}
                  <span class="code">{{ data.code }}</span>
                  <el-tag v-if="data.hot" size="small" type="danger" effect="plain" style="margin-left:4px">高危</el-tag>
                </span>
              </template>
            </el-tree>
            <el-text v-if="!curRole.code" size="small" type="info">← 选择角色</el-text>
          </el-card>
        </div>
      </div>
    </el-card>

    <!-- 新建角色 -->
    <el-dialog v-model="dlg" title="新建角色" width="460px">
      <el-form label-width="80px" size="small">
        <el-form-item label="角色 code"><el-input v-model="form.code" placeholder="大写, 如 RISK_SPEC" /></el-form-item>
        <el-form-item label="角色名"><el-input v-model="form.name" placeholder="如 风控专员" /></el-form-item>
        <el-form-item label="描述"><el-input v-model="form.desc" /></el-form-item>
      </el-form>
      <template #footer><el-button size="small" @click="dlg = false">取消</el-button><el-button size="small" type="primary" :loading="creating" @click="create">创建</el-button></template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, nextTick } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import request from '../../api/request'

const BUILTIN = ['ADMIN', 'OPERATOR', 'VIEWER', 'AUDITOR', 'LABEL_ENG', 'ANALYST']
const isBuiltin = (c) => BUILTIN.includes(c)

const roles = ref([])
const cur = ref('')
const tree = ref([])
const treeRef = ref(null)
const loading = ref(false)
const saving = ref(false)
const deleting = ref(false)
const creating = ref(false)
const saved = ref(false)
const dlg = ref(false)
const form = ref({ code: '', name: '', desc: '' })

const curRole = computed(() => roles.value.find(r => r.code === cur.value) || {})

async function loadRoles() {
  try {
    const data = await request.get('/roles')
    roles.value = (data || []).map(r => ({ id: r.id, code: r.roleCode, name: r.roleName, desc: r.description }))
    if (!cur.value && roles.value.length) { cur.value = roles.value[0].code; await loadPermsOf(curRole.value.id) }
  } catch (e) { /* 已提示 */ }
}
async function loadPermTree() {
  try {
    const mods = await request.get('/permissions')
    tree.value = (mods || []).map(m => ({
      id: 'm-' + m.module, label: m.module, type: 'group', module: m.module, children: []
    }))
    // 打平为两级: 组(group) → 全部菜单/动作 平铺? 按 module 分组后组内再分 menu/action?
    // 接口为 module → perms(flat)。构造成: group(module) > menu(无 children 或按序)。 简化: 两层= 组 > 权限点(type 标 menu/action)
    tree.value = (mods || []).map(m => ({
      id: 'g-' + m.module,
      label: m.module,
      type: 'group',
      children: (m.perms || []).map(p => ({ id: p.code, label: p.name, code: p.code, module: m.module, type: p.type, hot: p.hot }))
    }))
  } catch (e) { /* 403 已提示 */ }
}
const rolePerms = ref({})   // roleId -> Set codes
async function loadPermsOf(roleId) {
  if (!roleId) return
  try {
    const codes = await request.get(`/roles/${roleId}/perms`)
    rolePerms.value[roleId] = codes || []
  } catch (e) { /* 已提示 */ }
  nextTick(() => {
    const t = treeRef.value
    if (!t) return
    const all = []
    const walk = (n) => { (n.code ? [n] : []); (n.children || []).forEach(walk) }
    tree.value.forEach(walk)
    t.setCheckedKeys(rolePerms.value[roleId] || [])
  })
}
async function select(r) {
  cur.value = r.code
  await loadPermsOf(r.id)
}
async function save() {
  const t = treeRef.value
  if (!t || !curRole.value.id) return
  saving.value = true
  try {
    const codes = [...t.getCheckedKeys(), ...t.getHalfCheckedKeys()]
    await request.put(`/roles/${curRole.value.id}/perms`, codes)
    saved.value = true
    ElMessage.success(`已保存 ${codes.length} 个权限点`)
    setTimeout(() => (saved.value = false), 1200)
  } catch (e) { /* 已提示 */ } finally { saving.value = false }
}
function openCreate() { form.value = { code: '', name: '', desc: '' }; dlg.value = true }
async function create() {
  if (!form.value.code || !form.value.name) { ElMessage.warning('code 与名称必填'); return }
  creating.value = true
  try {
    await request.post('/roles', { roleCode: form.value.code.toUpperCase(), roleName: form.value.name, description: form.value.desc })
    ElMessage.success('角色已创建, 请勾选权限点后保存')
    dlg.value = false
    await loadRoles()
    const r = roles.value.find(x => x.code === form.value.code.toUpperCase())
    if (r) { cur.value = r.code; await loadPermsOf(r.id) }
  } catch (e) { /* 已提示 */ } finally { creating.value = false }
}
async function delRole() {
  if (!curRole.value.id) return
  await ElMessageBox.confirm('删除角色「' + curRole.value.name + '」? 预置角色不可删。', '删除角色', { type: 'warning' }).catch(() => Promise.reject())
  deleting.value = true
  try {
    await request.delete(`/roles/${curRole.value.id}`)
    ElMessage.success('已删除')
    cur.value = ''
    await loadRoles()
  } catch (e) { /* 后端提示(如预置不可删) */ } finally { deleting.value = false }
}

async function init() {
  loading.value = true
  try { await Promise.all([loadPermTree(), loadRoles()]) } finally { loading.value = false }
}
init()
</script>

<style scoped>
.page-card { max-width: 1500px; }
.pg-title { font-size: 17px; font-weight: 700; }
.head { display: flex; justify-content: space-between; align-items: flex-start; }
.flex { display: flex; }
.flex-between { display: flex; justify-content: space-between; align-items: center; }
.side { width: 300px; flex-shrink: 0; }
.side :deep(.el-card__body) { padding: 8px; }
.rrow { padding: 8px 10px; cursor: pointer; border-radius: 6px; margin-bottom: 2px; }
.rrow:hover { background: var(--el-fill-color-light); }
.rrow.on { background: var(--el-color-primary-light-9); }
.rname { display: flex; align-items: center; gap: 6px; }
.dim { color: #999; font-size: 12px; }
.perm-tip { background: #f0f7ff; border: 1px solid #c6e0f7; color: #2a5d8f; padding: 8px 10px; border-radius: 6px; font-size: 12px; margin-bottom: 10px; }
.pnode { font-size: 13px; }
.pnode .code { color: #aaa; font-size: 11px; margin-left: 6px; font-family: monospace; }
</style>

<template>
  <div class="bk-config">
    <el-alert type="info" :closable="false" show-icon style="margin-bottom: 14px"
      title="业务主键 = 路由键 + 收敛键:决定「哪些记录是同一个实体」,分桶/分表与幂等收敛(唯一键)都基于它。键构成上线后不可变,变更 = 实体分裂 = 全量重迁" />

    <!-- ① 键模式 -->
    <div class="sec-title">① 键模式(决定同一条业务记录的判定)</div>
    <el-radio-group v-model="cfg.mode">
      <el-radio-button value="merge">合并 merge(纯业务键)</el-radio-button>
      <el-radio-button value="scope">区分 scope(带来源作用域)</el-radio-button>
    </el-radio-group>
    <div class="mode-desc">
      <template v-if="cfg.mode === 'merge'">
        不同来源同一业务键(如身份证号)→ 合并为同一行,跨系统汇聚收敛;来源变化(主从切换/换库)不影响键
      </template>
      <template v-else>
        业务键 + 来源维度 → 同 id 不同来源 = 不同实体(如各系统订单号可能撞号);来源变化会改变键
      </template>
    </div>

    <!-- ② 来源维度(scope 模式) -->
    <template v-if="cfg.mode === 'scope'">
      <div class="sec-title mt16">② 来源维度(进键,区分来源)<el-text type="info" size="small">按数据源类型动态提供</el-text></div>
      <el-checkbox-group v-model="cfg.scopeFields">
        <el-checkbox v-for="opt in scopeOptions" :key="opt.value" :value="opt.value">{{ opt.label }}{{ opt.kind === 'physical' ? '(物理身份)' : opt.kind === 'source' ? '(来源级)' : '' }}</el-checkbox>
      </el-checkbox-group>

      <div v-if="cfg.scopeFields.includes('system_name')" class="sys-names">
        <div class="fc-label">系统名称取值(参与键的具体值,可新增)</div>
        <el-select v-model="cfg.systemNames" multiple filterable allow-create default-first-option
          placeholder="选择或输入系统名称(如 ERP / CRM),回车新增" style="width: 100%">
          <el-option v-for="s in systemOptions" :key="s" :value="s" :label="s" />
        </el-select>
        <div class="bk-tip">同一任务涉及多个系统时勾选全部;输入新名称回车即新增(写入系统字典),之后所有任务的候选里都会出现</div>
      </div>

      <div v-if="physicalSelected" class="bk-warn">
        ⚠ 已选物理身份维度({{ physicalLabels.join(' / ') }}):换库、主从切换、实例迁移都会导致键变化 → 同一实体分裂成两行、永远合并不了。请确认该数据源绝不会发生物理迁移,或改选「系统名称」。
        <div style="margin-top: 6px">
          <el-checkbox v-model="cfg.confirmPhysical">我确认:该数据源不会发生物理迁移/换库,接受物理身份进键</el-checkbox>
        </div>
      </div>
      <div v-if="cfg.scopeFields.includes('table_name')" class="bk-warn">
        ⚠ {{ tableLabel }}名进键:多源{{ tableLabel }}汇聚到同一目标时无法合并(同一实体来自不同源{{ tableLabel }} = 不同键)。仅当「同 id 跨{{ tableLabel }}是不同实体」时才应选择。
      </div>
    </template>

    <!-- ③ 业务键字段 -->
    <div class="sec-title mt16">③ 业务键字段(可多选组合)</div>
    <el-select v-model="cfg.keyFields" multiple filterable placeholder="选择模型字段作为业务键(如 id_card_no / order_id)" style="width: 100%">
      <el-option v-for="f in fields" :key="f.name" :value="f.name" :label="`${f.name} (${f.type})`" />
    </el-select>
    <div class="bk-tip">可组合多个字段(如 租户号 + 单号);主键会被修改的字段不建议作业务键(变更 = 实体分裂,需拆 删+插)</div>

    <!-- ④ 生效预览 -->
    <div class="sec-title mt16">④ 生效预览</div>
    <el-descriptions :column="1" border size="small" style="max-width: 760px">
      <el-descriptions-item label="键构成">
        <span class="mono">{{ prettyKeyParts.length ? prettyKeyParts.join(' + ') : '—(未配置)' }}</span>
      </el-descriptions-item>
      <el-descriptions-item label="目标唯一键">
        <span class="mono">{{ uniqueKeySql }}</span>
      </el-descriptions-item>
      <el-descriptions-item label="分桶 / 分表">
        <span class="mono">桶号 = hash(键) % 4096 · 物理表号 = hash(键) % 50</span>
      </el-descriptions-item>
      <el-descriptions-item label="路由示例">
        <span v-if="keyParts.length" class="mono">
          "{{ sampleSys[0] }}"+100231 → 表号 {{ demoTableNo(sampleSys[0] + '|100231') }} &nbsp;·&nbsp; "{{ sampleSys[1] }}"+100231 → 表号 {{ demoTableNo(sampleSys[1] + '|100231') }} &nbsp;·&nbsp;
          100231(无来源) → 表号 {{ demoTableNo('100231') }}
        </span>
        <span v-else>—</span>
      </el-descriptions-item>
    </el-descriptions>
    <div v-if="!valid" class="bk-error">⚠ {{ firstError }}</div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { dbLevels, scopeFieldOptions, PHYSICAL_FIELDS } from '../../../utils/dbLevels'

const props = defineProps({
  /** 候选字段:[{ name, type }] — 来自目标模型字段 */
  fields: { type: Array, default: () => [] },
  /** 数据源类型:mysql/postgresql/oracle/sqlserver/mongodb,决定来源维度选项 */
  dbType: { type: String, default: 'mysql' },
  /** 系统名称候选(来自数据源系统字典) */
  systemOptions: { type: Array, default: () => [] },
  /** 配置对象: { mode, scopeFields, keyFields, confirmPhysical } */
  modelValue: { type: Object, required: true }
})
const emit = defineEmits(['update:modelValue'])

const cfg = computed({
  get: () => props.modelValue,
  set: v => emit('update:modelValue', v)
})

const lv = computed(() => dbLevels(props.dbType))
const scopeOptions = computed(() => scopeFieldOptions(props.dbType))
const tableLabel = computed(() => lv.value.tableLabel)

const keyParts = computed(() => [
  ...(cfg.value.mode === 'scope' ? cfg.value.scopeFields : []),
  ...cfg.value.keyFields
])
// 展示用: system_name 带上具体取值,如 system_name[ERP|CRM]
const prettyKeyParts = computed(() => keyParts.value.map(p =>
  p === 'system_name' ? `system_name[${(cfg.value.systemNames || []).join('|') || '?'}]` : p))
// 路由示例用系统名(优先用已配置的取值)
const sampleSys = computed(() => {
  const names = cfg.value.scopeFields.includes('system_name') && cfg.value.systemNames?.length
    ? cfg.value.systemNames : ['ERP', 'CRM']
  return [names[0] || 'ERP', names[1] || names[0] || 'CRM']
})
const physicalSelected = computed(() => cfg.value.scopeFields.some(f => PHYSICAL_FIELDS.includes(f)))
const physicalLabels = computed(() =>
  cfg.value.scopeFields.filter(f => PHYSICAL_FIELDS.includes(f))
    .map(f => scopeOptions.value.find(o => o.value === f)?.label || f))
const uniqueKeySql = computed(() =>
  keyParts.value.length ? `UNIQUE KEY uk_biz (${keyParts.value.join(', ')})` : '—')

// 校验(与父组件 bkValid 保持一致)
const valid = computed(() => {
  if (!cfg.value.keyFields.length) return false
  if (cfg.value.mode === 'scope' && !cfg.value.scopeFields.length) return false
  if (cfg.value.mode === 'scope' && cfg.value.scopeFields.includes('system_name') && !(cfg.value.systemNames || []).length) return false
  if (physicalSelected.value && !cfg.value.confirmPhysical) return false
  return true
})
const firstError = computed(() => {
  if (!cfg.value.keyFields.length) return '请至少选择一个业务键字段(多字段可组合)'
  if (cfg.value.mode === 'scope' && !cfg.value.scopeFields.length) return '区分模式必须至少选择一个来源维度'
  if (cfg.value.mode === 'scope' && cfg.value.scopeFields.includes('system_name') && !(cfg.value.systemNames || []).length) return '已勾选系统名称,请选择或新增具体的系统名称取值'
  if (physicalSelected.value && !cfg.value.confirmPhysical) return '已选物理身份维度,需勾选确认(换库/迁移会导致实体分裂)'
  return ''
})

// 路由示例:确定性字符串散列(仅前端演示,与后端 RouteUtil 实现对齐思路)
function hashStr(s) {
  let h = 0x9747b28c
  for (let i = 0; i < s.length; i++) {
    h = Math.imul(h ^ s.charCodeAt(i), 0x85ebca6b)
    h = (h << 13) | (h >>> 19)
  }
  h ^= h >>> 16
  return Math.abs(h)
}
function demoTableNo(v) { return hashStr(v) % 50 }
</script>

<style scoped>
.bk-config { max-width: 860px; }
.sec-title { font-size: 14px; font-weight: 600; margin-bottom: 12px; }
.mt16 { margin-top: 16px; }
.mode-desc { margin-top: 8px; font-size: 12px; color: #8f959e; }
.bk-tip { margin-top: 6px; font-size: 12px; color: #8f959e; }
.sys-names { margin-top: 10px; padding: 10px 12px; background: #f6f8fa; border: 1px solid #e8e8e8; border-radius: 6px; }
.fc-label { font-size: 12.5px; font-weight: 600; color: #646a73; margin-bottom: 8px; }
.bk-warn { margin-top: 10px; padding: 8px 12px; background: #fffbe6; border: 1px solid #ffe58f; border-radius: 6px; font-size: 12px; color: #ad6800; }
.bk-error { margin-top: 10px; padding: 8px 12px; background: #fff1f0; border: 1px solid #ffa39e; border-radius: 6px; font-size: 12px; color: #cf1322; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 12px; }
</style>

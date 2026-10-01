<script setup>
import { ref } from 'vue'
import { store, nsColor } from '../store'
import { ElMessage } from 'element-plus'

const opType = ref('手动迁移分区')
const opNs = ref('sync')
const opTarget = ref('')
const opDetail = ref('')

const opTargetOptions = {
  '手动迁移分区': '如:dcp.cdc.orders 分区5 → sync-06',
  '强制扩缩容': '如:dcp.cdc.orders 扩容到 12 分区(豁免冷却)',
  '锁定分区': '如:dcp.cdc.target_db.payments 分区1 冻结自动调度',
  '调实例权重': '如:sync-04 权重 → 1.5',
  '实例下架': '如:sync-09 进入 DRAINING 迁空下线',
}

function now() {
  const d = new Date(); const p = n => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

function submit() {
  if (!opTarget.value) return ElMessage.warning('请填写指令内容')
  store.audits.unshift({ time: now(), user: 'admin', ns: opNs.value, action: opType.value, detail: opTarget.value })
  opTarget.value = ''
  ElMessage.success('指令已写入 etcd /ops/{id},调度主下一轮循环统一执行')
}
</script>

<template>
  <div>
    <h1 class="page-title">运维操作</h1>
    <p class="page-desc">人工指令写 etcd ops key,由调度主统一执行(与自动决策共用执行器);优先级:人工锁定 &gt; 人工强制 &gt; 自动扩缩;人工操作全部留审计。</p>

    <div class="two-col">
      <div class="card">
        <h3 class="card-title">下发人工指令</h3>
        <el-form label-width="90px">
          <el-form-item label="指令类型">
            <el-select v-model="opType" style="width:100%">
              <el-option v-for="t in Object.keys(opTargetOptions)" :key="t" :label="t" :value="t" />
            </el-select>
          </el-form-item>
          <el-form-item label="namespace">
            <el-select v-model="opNs" style="width:100%">
              <el-option v-for="ns in store.namespaces" :key="ns.name" :label="`${ns.label} (${ns.name})`" :value="ns.name" />
            </el-select>
          </el-form-item>
          <el-form-item label="指令内容">
            <el-input v-model="opTarget" type="textarea" :rows="3" :placeholder="opTargetOptions[opType]" />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="submit">下发指令(写 ops key)</el-button>
          </el-form-item>
        </el-form>
        <div class="boundary">
          <b>后台边界(不能做的)</b>:编写钩子逻辑(业务代码)· 运行时把实例换 namespace(部署属性)· 改变 min/max 并发边界的业务含义(业务契约锁死)。
        </div>
      </div>

      <div class="card">
        <h3 class="card-title">审计记录</h3>
        <el-table :data="store.audits" size="small">
          <el-table-column prop="time" label="时间" width="140" />
          <el-table-column prop="user" label="操作人" width="90" />
          <el-table-column label="ns" width="80">
            <template #default="{ row }"><span :style="{ color: nsColor[row.ns] || '#0f172a', fontWeight: 600 }">{{ row.ns }}</span></template>
          </el-table-column>
          <el-table-column prop="action" label="动作" width="120" />
          <el-table-column prop="detail" label="详情" min-width="220" />
        </el-table>
      </div>
    </div>

    <div class="tip">
      <b>权限模型</b>:平台管理员(全 ns 读写:创建 ns/下架实例/归档)· 场景管理员(单 ns 读写)· 观察者(单 ns 只读);权限绑定到 namespace,与"互不可见"的隔离语义对齐。
    </div>
  </div>
</template>

<style scoped>
.page-title { font-size: 22px; font-weight: 700; color: #0f172a; }
.page-desc { color: #64748b; font-size: 13px; margin: 8px 0 18px; max-width: 860px; }
.two-col { display: grid; grid-template-columns: 1fr 1.4fr; gap: 16px; }
.card { background: #fff; border-radius: 12px; padding: 18px 20px; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.card-title { font-size: 15px; color: #0f172a; margin-bottom: 12px; }
.boundary { background: #fffbeb; border: 1px solid #fde68a; border-radius: 10px; padding: 10px 14px; font-size: 12px; color: #78350f; line-height: 1.8; }
.tip { margin-top: 16px; background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 10px; padding: 12px 16px; font-size: 12.5px; color: #1e3a8a; line-height: 1.8; }
</style>

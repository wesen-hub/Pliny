<template>
  <div class="page-card">
    <el-card shadow="never">
      <template #header>新建订阅(标签条件 → 动态命中资产)</template>
      <el-form label-width="110px" style="max-width: 720px">
        <el-form-item label="订阅名称"><el-input v-model="form.name" placeholder="如: 交易宽表订阅" /></el-form-item>
        <el-form-item label="标签条件">
          <div style="width: 100%">
            <el-select v-model="form.op" size="small" style="width: 90px; margin-bottom: 8px">
              <el-option label="AND(全部命中)" value="AND" /><el-option label="OR(任一命中)" value="OR" />
            </el-select>
            <el-tree-select v-model="form.tags" :data="tagTree" multiple filterable style="width: 100%"
              :render-after-expand="false" :props="{ label: 'path', value: 'path', children: 'children' }"
              check-strictly placeholder="选择标签(多选, 含子孙继承)" />
          </div>
        </el-form-item>
        <el-form-item label="密级上限">
          <el-select v-model="form.maxSecret" style="width: 200px">
            <el-option label="不限制" :value="null" />
            <el-option label="公开" :value="1" /><el-option label="内部" :value="2" />
            <el-option label="机密" :value="3" /><el-option label="绝密" :value="4" />
          </el-select>
        </el-form-item>
        <el-form-item label="模式">
          <el-radio-group v-model="form.mode">
            <el-radio value="pull">Pull(主动拉取)</el-radio><el-radio value="push">Push(推送)</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="接收方"><el-input v-model="form.target" placeholder="如: 数据应用组 / 华东子节点" /></el-form-item>
        <el-form-item label="命中预览">
          <el-button size="small" :loading="previewLoading" @click="preview">实时预览命中资产</el-button>
          <div v-if="previewAssets" style="margin-top: 8px; width: 100%">
            <el-tag v-for="a in previewAssets" :key="a.table" size="small" style="margin-right:6px">{{ a.table }}</el-tag>
            <div class="sub">共 {{ previewAssets.length }} 个资产(标签动态子集, 资产变更自动跟随)</div>
          </div>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :disabled="!form.name || !form.tags.length" @click="submit">创建订阅</el-button>
          <el-button @click="$router.back()">取消</el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { getTagTree, getTagSubset, createSubscription } from '../../api'

const router = useRouter()
const tagTree = ref([])
const previewAssets = ref(null)
const previewLoading = ref(false)
const form = ref({ name: '', tags: [], op: 'AND', maxSecret: null, mode: 'pull', target: '' })

async function preview() {
  if (!form.value.tags.length) return
  previewLoading.value = true
  try {
    let assets = await getTagSubset(form.value.tags, form.value.op) || []
    if (form.value.maxSecret) {
      assets = assets.filter(a => a.secretLevel <= form.value.maxSecret)
    }
    previewAssets.value = assets
  } finally {
    previewLoading.value = false
  }
}

async function submit() {
  await createSubscription({
    name: form.value.name,
    expr: { tags: form.value.tags, op: form.value.op, exclude: [], maxSecret: form.value.maxSecret },
    mode: form.value.mode,
    target: form.value.target || '数据应用组'
  })
  ElMessage.success('订阅已创建')
  router.push('/subscribe')
}

onMounted(async () => {
  tagTree.value = await getTagTree() || []
})
</script>

<style scoped>
.sub { color: #8f959e; font-size: 12px; margin-top: 4px; }
</style>

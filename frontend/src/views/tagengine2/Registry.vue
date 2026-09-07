<template>
  <div class="page-card">
    <el-alert type="info" :closable="false" show-icon style="margin-bottom:14px"
      title="标签注册表(V2 · mock): 标签=一等资产, 在此声明 生成方式/结构/时效/存储; 打标任务引用注册表, 引擎按声明路由执行与落库" />
    <el-card shadow="never">
      <div class="toolbar">
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <el-input v-model="q.kw" placeholder="标签名" clearable style="width:160px" />
          <el-select v-model="q.category" placeholder="生成方式" clearable style="width:130px">
            <el-option v-for="(v, k) in categoryMap" :key="k" :value="k" :label="v" />
          </el-select>
          <el-select v-model="q.timing" placeholder="时效" clearable style="width:130px">
            <el-option v-for="(v, k) in timingMap" :key="k" :value="k" :label="v" />
          </el-select>
          <el-button type="primary" @click="toast('筛选(mock)')">查询</el-button>
        </div>
        <div>
          <el-button type="primary" @click="go('/tagengine2/task/create')">＋ 新建打标任务(V2 向导)</el-button>
          <el-button type="warning" @click="openCreate">＋ 注册标签</el-button>
        </div>
      </div>

      <el-table :data="tags" stripe>
        <el-table-column label="标签(层级路径)" min-width="210">
          <template #default="{ row }">
            <el-link type="primary">{{ row.path }}</el-link>
            <el-text size="small" type="info" style="display:block;font-size:11px">{{ row.owner }} · v{{ row.version }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="生成方式" width="100">
          <template #default="{ row }"><el-tag size="small" effect="plain" :type="catType(row.category)">{{ categoryMap[row.category] }}</el-tag></template>
        </el-table-column>
        <el-table-column label="结构" width="150">
          <template #default="{ row }">
            <el-tag size="small" type="info" effect="plain">{{ valueTypeMap[row.valueType] }}</el-tag>
            <el-tag size="small" effect="plain" style="margin-left:4px">{{ row.cardinality === 'single' ? '单值' : '多值' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="时效" width="120">
          <template #default="{ row }">
            <el-tag size="small" :type="row.timing === 'sliding' || row.timing === 'instant' ? 'danger' : 'warning'" effect="plain">{{ timingMap[row.timing] }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="计算 / 存储" min-width="150">
          <template #default="{ row }">
            <div><el-tag size="small" effect="plain">{{ computeMap[row.compute] }}</el-tag>
              <el-tag v-for="s in row.store" :key="s" size="small" type="success" effect="plain" style="margin-left:4px">{{ storeMap[s] }}</el-tag>
            </div>
            <el-text size="small" type="info" style="font-size:10px">ttl {{ row.ttl }} · 刷新 {{ row.refresh }}</el-text>
          </template>
        </el-table-column>
        <el-table-column prop="hits" label="覆盖实体" width="110" />
        <el-table-column label="操作" width="120">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="toast('查看口径(mock)')">口径</el-link>
            <el-link type="success" underline="never" style="margin-left:8px" @click="toast('建任务(mock)')">建任务</el-link>
            <el-link type="danger" underline="never" style="margin-left:8px" @click="toast('下线(mock)')">下线</el-link>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 注册标签 -->
    <el-dialog v-model="dlg" title="注册标签(声明其类型/时效/存储)" width="720px">
      <el-form label-width="120px">
        <el-form-item label="标签路径" required>
          <el-select v-model="form.path" filterable style="width:340px" placeholder="选择/输入层级标签路径">
            <el-option v-for="p in paths" :key="p" :value="p" :label="p" />
          </el-select>
          <el-text size="small" type="info" style="display:block">层级=父链自动继承(tag_dict 树), 如 客户分层/高价值</el-text>
        </el-form-item>
        <el-form-item label="生成方式">
          <el-radio-group v-model="form.category">
            <el-radio-button v-for="(v, k) in categoryMap" :key="k" :value="k">{{ v }}</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="结构">
          <el-select v-model="form.valueType" style="width:220px">
            <el-option v-for="(v, k) in valueTypeMap" :key="k" :value="k" :label="v" />
          </el-select>
          <el-radio-group v-model="form.cardinality" style="margin-left:16px">
            <el-radio-button value="single">单值</el-radio-button>
            <el-radio-button value="multi">多值</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="时效">
          <el-radio-group v-model="form.timing" @change="onTiming">
            <el-radio-button v-for="(v, k) in timingMap" :key="k" :value="k">{{ v }}</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="计算模式">
          <el-tag size="large" effect="plain" :type="form.compute === 'online' ? 'success' : form.compute === 'stream' ? 'danger' : 'warning'">{{ computeMap[form.compute] }}</el-tag>
          <el-text size="small" type="info" style="margin-left:10px">{{ computeHint }}</el-text>
        </el-form-item>
        <el-form-item label="存储层次">
          <el-checkbox v-for="s in ['fact', 'online', 'bitmap']" :key="s" v-model="form.store" :value="s" style="margin-right:10px">{{ storeMap[s] }}</el-checkbox>
          <el-text size="small" type="info" style="display:block;font-size:11px">引擎按声明路由: 批写事实, 流写事实+在线, 瞬时仅在线(Redis)</el-text>
        </el-form-item>
        <el-form-item label="ttl / 刷新">
          <el-select v-model="form.ttl" style="width:130px"><el-option value="永久" label="永久" /><el-option value="1d" label="1 天" /><el-option value="5m" label="5 分钟" /></el-select>
          <el-input v-model="form.refresh" size="small" style="width:240px;margin-left:10px" placeholder="cron / 事件源 / 常驻" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dlg = false">取消</el-button>
        <el-button type="primary" @click="toast('注册成功(mock): 标签已入注册表, 可建任务')">注册</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'

const router = useRouter()
const go = (p) => router.push(p)
const toast = (m) => ElMessage.info(m)
const q = ref({})
const dlg = ref(false)
const categoryMap = { fact: '事实', rule: '规则', stat: '统计', model: '模型', derive: '衍生' }
const valueTypeMap = { boolean: '布尔', enum1: '枚举·单值', enumN: '枚举·多值', number: '数值(指标)', score: '评分', datetime: '时间' }
const timingMap = { static: '静态', slow: '慢变', accumulate: '累积', sliding: '滑动窗口', instant: '瞬时' }
const computeMap = { batch: '批(Batch)', stream: '流(Stream)', online: '在线(Online)' }
const storeMap = { fact: '事实层', online: '在线层', bitmap: '位图' }
const catType = (c) => ({ fact: 'info', rule: 'success', stat: 'warning', model: 'danger', derive: 'primary' }[c])
const paths = ['客户分层/高价值', '客户分层/高频购买', '密级/数据密级', '兴趣/运动', '行为/近5分钟点击量', '状态/当前在线', '模型/流失概率', '区域/华东', '注册/注册城市', '生命周期/累计注册天数']
const tags = ref([])

const form = ref({ path: '', category: 'rule', valueType: 'boolean', cardinality: 'single', timing: 'slow', compute: 'batch', store: ['fact'], ttl: '永久', refresh: '' })
const computeHint = ref('')
function onTiming() {
  const map = { static: 'batch', slow: 'batch', accumulate: 'batch', sliding: 'stream', instant: 'online' }
  form.value.compute = map[form.value.timing]
  form.value.store = form.value.timing === 'instant' ? ['online'] : (form.value.timing === 'sliding' ? ['fact', 'online'] : ['fact'])
  computeHint.value = { batch: '批任务(Spark/SQL)定时/手动计算', stream: '流任务(Flink)订阅事件源+窗口', online: '在线事件置位(Redis TTL)' }[form.value.compute]
}
function openCreate() {
  form.value = { path: '', category: 'rule', valueType: 'boolean', cardinality: 'single', timing: 'slow', compute: 'batch', store: ['fact'], ttl: '永久', refresh: '' }
  dlg.value = true
}

onMounted(() => {
  tags.value = [
    { path: '注册/注册城市', owner: '数仓组', version: 3, category: 'fact', valueType: 'enum1', cardinality: 'single', timing: 'static', compute: 'batch', store: ['fact'], ttl: '永久', refresh: '全量每日', hits: '36.2 万' },
    { path: '客户分层/高频购买', owner: '运营组', version: 5, category: 'rule', valueType: 'boolean', cardinality: 'single', timing: 'slow', compute: 'batch', store: ['fact'], ttl: '1 天', refresh: '每日 02:00', hits: '8.1 万' },
    { path: '密级/数据密级', owner: '管控组', version: 2, category: 'fact', valueType: 'enum1', cardinality: 'single', timing: 'slow', compute: 'batch', store: ['fact'], ttl: '永久', refresh: '每日 05:00', hits: '36.2 万' },
    { path: '统计/近90天消费额', owner: '数科组', version: 4, category: 'stat', valueType: 'number', cardinality: 'single', timing: 'accumulate', compute: 'batch', store: ['fact'], ttl: '永久', refresh: '每日 03:00', hits: '33.1 万' },
    { path: '兴趣/运动', owner: '运营组', version: 1, category: 'rule', valueType: 'enumN', cardinality: 'multi', timing: 'slow', compute: 'batch', store: ['fact'], ttl: '7 天', refresh: '每周 04:00', hits: '12.4 万' },
    { path: '行为/近5分钟点击量', owner: '实时组', version: 2, category: 'stat', valueType: 'number', cardinality: 'single', timing: 'sliding', compute: 'stream', store: ['fact', 'online'], ttl: '1 天', refresh: '事件流常驻', hits: '实时' },
    { path: '状态/当前在线', owner: '实时组', version: 1, category: 'rule', valueType: 'boolean', cardinality: 'single', timing: 'instant', compute: 'online', store: ['online'], ttl: '5 分钟', refresh: '心跳事件', hits: '实时' },
    { path: '模型/流失概率', owner: '数科组', version: 7, category: 'model', valueType: 'score', cardinality: 'single', timing: 'slow', compute: 'batch', store: ['fact', 'bitmap'], ttl: '1 天', refresh: '模型发布后', hits: '36.2 万' },
    { path: '衍生/核心价值用户', owner: '运营组', version: 1, category: 'derive', valueType: 'boolean', cardinality: 'single', timing: 'slow', compute: 'batch', store: ['fact'], ttl: '1 天', refresh: '依赖上游完成后', hits: '5.9 万' },
    { path: '生命周期/累计注册天数', owner: '数仓组', version: 1, category: 'fact', valueType: 'number', cardinality: 'single', timing: 'accumulate', compute: 'batch', store: ['fact'], ttl: '永久', refresh: '每日全量', hits: '36.2 万' }
  ]
})
</script>

<style scoped>
.toolbar { display: flex; justify-content: space-between; margin-bottom: 14px; align-items: center; flex-wrap: wrap; gap: 8px; }
</style>

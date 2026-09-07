<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="display: flex; gap: 8px; flex-wrap: wrap">
          <el-select v-model="query.module" placeholder="模块" clearable style="width: 140px" @change="load">
            <el-option v-for="m in ['datasource', 'job', 'dict', 'alarm', 'system']" :key="m" :label="m" :value="m" />
          </el-select>
          <el-select v-model="query.operation" placeholder="操作" clearable style="width: 140px" @change="load">
            <el-option v-for="o in ['CREATE', 'UPDATE', 'DELETE', 'ENABLE', 'DISABLE', 'RUN', 'STOP', 'LOGIN', 'COPY', 'RESET_OFFSET', 'IMPORT']" :key="o" :label="o" :value="o" />
          </el-select>
          <el-input v-model="query.username" placeholder="操作人" clearable style="width: 130px" @keyup.enter="load" />
          <el-button type="primary" @click="load"><el-icon><Search /></el-icon>查询</el-button>
        </div>
      </div>
      <el-table :data="rows" v-loading="loading" stripe>
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="username" label="操作人" width="120" />
        <el-table-column prop="module" label="模块" width="110" />
        <el-table-column label="操作" width="120">
          <template #default="{ row }">
            <el-tag size="small">{{ row.operation }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="targetType" label="对象" width="110" />
        <el-table-column prop="targetId" label="对象ID" width="80" />
        <el-table-column prop="detail" label="详情" min-width="240" show-overflow-tooltip />
        <el-table-column prop="ip" label="IP" width="120" />
        <el-table-column prop="createTime" label="时间" width="160" />
      </el-table>
      <div class="pagination">
        <el-pagination background layout="total, prev, pager, next" :total="total"
                       :page-size="query.pageSize" :current-page="query.page" @current-change="p => { query.page = p; load() }" />
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import { listAuditLogs } from '../../api'

const rows = ref([])
const total = ref(0)
const loading = ref(false)
const query = reactive({ page: 1, pageSize: 10, module: '', operation: '', username: '' })

async function load() {
  loading.value = true
  try {
    const data = await listAuditLogs(query)
    rows.value = data.list
    total.value = data.total
  } finally {
    loading.value = false
  }
}

onMounted(load)
</script>

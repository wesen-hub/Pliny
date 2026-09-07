<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <el-input v-model="query.keyword" placeholder="搜索用户名" clearable style="width: 180px" @keyup.enter="load" />
        <el-button type="success" @click="openForm()"><el-icon><Plus /></el-icon>新建用户</el-button>
      </div>
      <el-table :data="rows" v-loading="loading" stripe>
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="username" label="用户名" width="150" />
        <el-table-column prop="nickname" label="昵称" width="140" />
        <el-table-column label="角色" min-width="140">
          <template #default="{ row }">
            <el-tag v-for="r in row.roles" :key="r" size="small" style="margin-right: 4px">{{ r }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="email" label="邮箱" min-width="150" />
        <el-table-column label="状态" width="80">
          <template #default="{ row }">
            <el-tag :type="row.status === 1 ? 'success' : 'info'" size="small">{{ row.status === 1 ? '正常' : '禁用' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="createTime" label="创建时间" width="160" />
        <el-table-column label="操作" width="200" fixed="right">
          <template #default="{ row }">
            <el-button size="small" type="primary" @click="openForm(row)">编辑</el-button>
            <el-button size="small" type="danger" @click="remove(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div class="pagination">
        <el-pagination background layout="total, prev, pager, next" :total="total"
                       :page-size="query.pageSize" :current-page="query.page" @current-change="p => { query.page = p; load() }" />
      </div>
    </el-card>

    <el-dialog v-model="dialog" :title="form.id ? '编辑用户' : '新建用户'" width="460px">
      <el-form :model="form" label-width="80px">
        <el-form-item label="用户名" required>
          <el-input v-model="form.username" :disabled="!!form.id" />
        </el-form-item>
        <el-form-item :label="form.id ? '重置密码' : '密码'">
          <el-input v-model="form.password" type="password" show-password
                    :placeholder="form.id ? '留空则不修改' : '必填'" />
        </el-form-item>
        <el-form-item label="昵称">
          <el-input v-model="form.nickname" />
        </el-form-item>
        <el-form-item label="邮箱">
          <el-input v-model="form.email" />
        </el-form-item>
        <el-form-item label="角色" required>
          <el-checkbox-group v-model="form.roles">
            <el-checkbox v-for="r in roles" :key="r.roleCode" :value="r.roleCode">{{ r.roleName }}</el-checkbox>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item label="状态">
          <el-switch v-model="form.status" :active-value="1" :inactive-value="0" active-text="正常" inactive-text="禁用" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialog = false">取消</el-button>
        <el-button type="primary" @click="save">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listUsers, createUser, updateUser, deleteUser, listRoles } from '../../api'

const rows = ref([])
const total = ref(0)
const loading = ref(false)
const roles = ref([])
const dialog = ref(false)
const query = reactive({ page: 1, pageSize: 10, keyword: '' })
const form = reactive({ id: null, username: '', password: '', nickname: '', email: '', roles: [], status: 1 })

async function load() {
  loading.value = true
  try {
    const data = await listUsers(query)
    rows.value = data.list
    total.value = data.total
  } finally {
    loading.value = false
  }
}

function openForm(row) {
  Object.assign(form, row ? {
    id: row.id, username: row.username, password: '', nickname: row.nickname,
    email: row.email, roles: [...row.roles], status: row.status
  } : { id: null, username: '', password: '', nickname: '', email: '', roles: [], status: 1 })
  dialog.value = true
}

async function save() {
  if (form.id) {
    await updateUser(form.id, form)
  } else {
    await createUser(form)
  }
  ElMessage.success('已保存')
  dialog.value = false
  load()
}

async function remove(row) {
  await ElMessageBox.confirm(`确定删除用户「${row.username}」?`, '提示', { type: 'warning' })
  await deleteUser(row.id)
  ElMessage.success('已删除')
  load()
}

onMounted(async () => {
  load()
  roles.value = await listRoles()
})
</script>

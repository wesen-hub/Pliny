<template>
  <div class="login-wrap">
    <el-card class="login-card">
      <div class="login-title">
        <el-icon :size="36" color="#409EFF"><DataAnalysis /></el-icon>
        <h2>数据采集平台</h2>
        <p>配置驱动 · 多源异构 · 零代码同步</p>
      </div>
      <el-form :model="form" @keyup.enter="onLogin">
        <el-form-item>
          <el-input v-model="form.username" placeholder="用户名" size="large" :prefix-icon="User" />
        </el-form-item>
        <el-form-item>
          <el-input v-model="form.password" type="password" placeholder="密码" size="large" :prefix-icon="Lock" show-password />
        </el-form-item>
        <el-button type="primary" size="large" style="width: 100%" :loading="loading" @click="onLogin">
          登 录
        </el-button>
      </el-form>
      <el-divider />
      <el-alert type="info" :closable="false" title="默认账号: admin / admin123" />
    </el-card>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { User, Lock } from '@element-plus/icons-vue'
import { useUserStore } from '../store/user'

const router = useRouter()
const userStore = useUserStore()
const form = reactive({ username: 'admin', password: 'admin123' })
const loading = ref(false)

async function onLogin() {
  if (!form.username || !form.password) {
    ElMessage.warning('请输入用户名和密码')
    return
  }
  loading.value = true
  try {
    await userStore.login(form.username, form.password)
    ElMessage.success('登录成功')
    router.push('/')
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.login-wrap {
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #1f2d3d 0%, #409EFF 100%);
}
.login-card { width: 400px; padding: 12px 8px; border-radius: 10px; }
.login-title { text-align: center; margin-bottom: 24px; }
.login-title h2 { margin: 8px 0 4px; color: #303133; }
.login-title p { color: #909399; font-size: 13px; }
</style>

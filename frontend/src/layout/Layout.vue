<template>
  <el-container style="height: 100%">
    <el-aside width="210px" class="aside">
      <div class="logo">
        <el-icon :size="24" color="#409EFF"><DataAnalysis /></el-icon>
        <span>数据采集平台</span>
      </div>
      <el-menu :default-active="activeMenu" router background-color="#001529" text-color="#a6adb4"
               active-text-color="#409EFF" style="border-right: none">
        <el-menu-item index="/dashboard"><el-icon><Odometer /></el-icon><span>工作台</span></el-menu-item>
        <el-menu-item index="/datasource"><el-icon><Connection /></el-icon><span>数据源管理</span></el-menu-item>
        <el-menu-item index="/dict"><el-icon><Collection /></el-icon><span>字典管理</span></el-menu-item>
        <el-menu-item index="/monitor/execution"><el-icon><List /></el-icon><span>执行历史</span></el-menu-item>
        <el-menu-item index="/monitor/stats"><el-icon><TrendCharts /></el-icon><span>统计报表</span></el-menu-item>
        <el-sub-menu index="sync">
          <template #title><el-icon><DataLine /></el-icon><span>数据同步</span></template>
          <el-menu-item index="/sync/overview">同步总览</el-menu-item>
          <el-menu-item index="/model">模型管理</el-menu-item>
          <el-menu-item index="/sync/task">同步任务</el-menu-item>
          <el-menu-item index="/filesync/task">文件同步</el-menu-item>
          <el-menu-item index="/agent">采集 Agent</el-menu-item>
          <el-menu-item index="/ingest/app">事件接入</el-menu-item>
        </el-sub-menu>
        <el-sub-menu index="asset">
          <template #title><el-icon><Files /></el-icon><span>数据资产</span></template>
          <el-menu-item index="/asset/catalog">资产目录</el-menu-item>
          <el-menu-item index="/asset/lineage">数据血缘</el-menu-item>
        </el-sub-menu>
        <el-sub-menu index="subscribe">
          <template #title><el-icon><Promotion /></el-icon><span>订阅与分发</span></template>
          <el-menu-item index="/subscribe">订阅中心</el-menu-item>
          <el-menu-item index="/subscribe/create">新建订阅</el-menu-item>
        </el-sub-menu>
        <el-sub-menu index="govern">
          <template #title><el-icon><Management /></el-icon><span>数据管控</span></template>
          <el-menu-item index="/tag">标签管理</el-menu-item>
          <el-menu-item v-if="userStore.isAdmin" index="/system/permission">权限管理</el-menu-item>
        </el-sub-menu>
        <template v-if="userStore.isAdmin">
          <el-menu-item index="/system/user"><el-icon><User /></el-icon><span>用户管理</span></el-menu-item>
          <el-menu-item index="/system/audit"><el-icon><Document /></el-icon><span>审计日志</span></el-menu-item>
        </template>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header class="header">
        <div class="header-title">{{ currentTitle }}</div>
        <el-dropdown @command="onCommand">
          <span class="user-info">
            <el-icon><UserFilled /></el-icon>
            {{ userStore.user?.nickname || userStore.user?.username }}
            <el-icon><ArrowDown /></el-icon>
          </span>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item disabled>角色: {{ (userStore.roles || []).join(' / ') }}</el-dropdown-item>
              <el-dropdown-item divided command="logout">退出登录</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </el-header>
      <el-main style="padding: 0; background: #f0f2f5">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessageBox } from 'element-plus'
import { useUserStore } from '../store/user'

const route = useRoute()
const router = useRouter()
const userStore = useUserStore()

const activeMenu = computed(() => route.meta.active || route.path)
const currentTitle = computed(() => route.meta.title || '数据采集平台')

onMounted(() => {
  if (!userStore.user) {
    userStore.fetchMe()
  }
})

async function onCommand(cmd) {
  if (cmd === 'logout') {
    await ElMessageBox.confirm('确定退出登录吗?', '提示', { type: 'warning' })
    userStore.logout()
    router.push('/login')
  }
}
</script>

<style scoped>
.aside { background: #001529; }
.logo { height: 56px; display: flex; align-items: center; justify-content: center; gap: 8px; color: #fff; font-weight: 600; }
.header { background: #fff; display: flex; align-items: center; justify-content: space-between; box-shadow: 0 1px 4px rgba(0,21,41,.08); }
.header-title { font-size: 16px; font-weight: 600; color: #303133; }
.user-info { display: flex; align-items: center; gap: 6px; cursor: pointer; color: #606266; }
</style>

<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="font-weight: 600">权限策略<el-text type="info" style="margin-left: 8px; font-weight: 400">Apache Ranger 集中配置 · 表/列/行级 · 动态脱敏</el-text></div>
        <el-button type="primary" @click="ElMessage.info('新建策略(规划中)')">＋ 新建策略</el-button>
      </div>
      <el-table :data="policies" stripe>
        <el-table-column prop="name" label="策略" min-width="160" />
        <el-table-column prop="resource" label="资源" min-width="150" />
        <el-table-column label="粒度" width="130">
          <template #default="{ row }"><el-tag size="small" :type="row.level === '行级' ? 'warning' : 'primary'">{{ row.level }}</el-tag></template>
        </el-table-column>
        <el-table-column prop="perm" label="权限" width="90" />
        <el-table-column label="脱敏" width="130">
          <template #default="{ row }">
            <el-tag v-if="row.mask" size="small" type="warning">{{ row.mask }}</el-tag>
            <span v-else>—</span>
          </template>
        </el-table-column>
        <el-table-column prop="who" label="适用角色/子节点" min-width="180" />
        <el-table-column label="状态" width="90">
          <template #default="{ row }"><el-tag size="small" :type="row.status === '启用' ? 'success' : 'warning'" effect="plain">{{ row.status }}</el-tag></template>
        </el-table-column>
      </el-table>
      <div class="tip">订阅审批通过时自动创建 Ranger 策略(逻辑视图)或绑定投递密级上限(物理同步);数据分发强制携带密级标签,审计可追溯。</div>
    </el-card>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
// TODO: 接入后端 API(/policies,Ranger 同步)
const policies = ref([
  { name: '华东子节点-订单查询', resource: 'DWD_订单明细', level: '表级+行级', perm: 'SELECT', mask: '手机号掩码', who: '华东子节点', status: '启用' },
  { name: '财务分析组', resource: 'DWD_*', level: '表级', perm: 'SELECT', mask: '金额脱敏', who: 'role: finance-analyst', status: '启用' },
  { name: '行级过滤-华东', resource: 'DWD_订单明细', level: '行级', perm: 'SELECT', mask: '', who: "region='east'", status: '启用' },
  { name: '列级-手机号', resource: 'DWD_客户主数据', level: '列级', perm: 'SELECT', mask: '掩码+加密', who: '除数据Owner外', status: '启用' },
  { name: 'sub-scm-material', resource: 'DWS_库存汇总', level: '订阅策略', perm: '投递', mask: '', who: '华中子节点', status: '订阅关联' }
])
</script>

<style scoped>
.tip { margin-top: 12px; font-size: 12px; color: #8f959e; }
</style>

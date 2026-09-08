<template>
  <div class="page-card">
    <el-card shadow="never">
      <!-- ═══ 页头: 主体选择全局共享 ═══ -->
      <div class="head">
        <div>
          <div class="pg-title">数据权限配置<el-tag size="small" effect="plain" style="margin-left:8px">设计稿 v0.2 · 演示数据</el-tag></div>
          <el-text size="small" type="info">维度分层: 资源级(业务分类默认档)→ 表级(行列+行规则)→ 标签级; 显式 > 分类默认 > 资产密级兜底; Deny 优先。所有配置均针对具体 <b>角色或用户</b>。</el-text>
        </div>
        <div class="subject-bar">
          <span class="lbl">授权对象</span>
          <el-radio-group v-model="subjectKind" size="small">
            <el-radio-button label="role">角色</el-radio-button>
            <el-radio-button label="user">用户</el-radio-button>
          </el-radio-group>
          <el-select v-model="subject" size="small" style="width:150px">
            <template v-if="subjectKind === 'role'">
              <el-option v-for="r in roles" :key="r" :label="r + '（角色）'" :value="r" />
            </template>
            <template v-else>
              <el-option v-for="u in users" :key="u.id" :label="u.name + '（' + u.role + '）'" :value="u.name" />
            </template>
          </el-select>
          <el-tag v-if="subjectDefault" size="small" type="info" effect="plain">{{ subjectDefault }}</el-tag>
        </div>
      </div>

      <el-tabs v-model="tab" style="margin-top:8px">
        <!-- ═══════════ T1 资源级(业务分类) ═══════════ -->
        <el-tab-pane label="① 资源级 · 业务分类" name="scope">
          <div class="panel-tip">
            分类维度默认档: 按 <b>业务域 / 密级 / 来源系统</b> 批量兜底, 作用于其下全部表与标签; 下层(表/标签级)显式配置可覆盖, Deny(L0)不可被下层放开。
          </div>
          <el-radio-group v-model="scopeDim" size="small" style="margin:8px 0">
            <el-radio-button label="biz">业务域</el-radio-button>
            <el-radio-button label="sec">密级</el-radio-button>
            <el-radio-button label="src">来源系统</el-radio-button>
          </el-radio-group>
          <el-table :data="scopeRows" size="small" border>
            <el-table-column label="分类" min-width="240">
              <template #default="{ row }">
                <el-icon style="vertical-align:-2px"><FolderOpened /></el-icon>
                <b>{{ row.path }}</b>
                <el-tag v-if="row.secret" size="small" effect="plain" style="margin-left:6px" :type="secType(row.secret)">{{ secName(row.secret) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="覆盖资源" width="110"><template #default="{ row }">{{ row.cover }}</template></el-table-column>
            <el-table-column label="能力档({{ subjectLabel }})" width="210">
              <template #default="{ row }">
                <el-select :model-value="row.level" size="small" style="width:140px" @change="(v) => (row.level = v)">
                  <el-option v-for="(lv, i) in levelNames" :key="i" :value="i"><span :class="'lv lv' + i">{{ lv }}</span></el-option>
                </el-select>
                <el-tag size="small" effect="plain" style="margin-left:4px" :type="row.eff === 'explicit' ? 'success' : 'info'">{{ row.eff === 'explicit' ? '显式' : '系统默认' }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="说明" min-width="240"><template #default="{ row }"><el-text size="small" type="info">{{ row.desc }}</el-text></template></el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ═══════════ T2 表级 · 行列 ═══════════ -->
        <el-tab-pane label="② 表级 + 行列级" name="table">
          <div class="flex" style="gap:12px;align-items:flex-start">
            <el-card shadow="never" class="side" style="width:270px">
              <template #header><b>数据集</b>(资产密级)</template>
              <div v-for="t in tables" :key="t.name" class="trow" :class="{ on: curTable === t.name }" @click="curTable = t.name">
                <el-icon style="vertical-align:-2px"><Grid /></el-icon>
                {{ t.name }}
                <el-tag size="small" effect="plain" style="margin-left:auto" :type="secType(t.secret)">{{ secName(t.secret) }}</el-tag>
              </div>
            </el-card>

            <div style="flex:1;min-width:0">
              <template v-if="cur">
                <!-- 表级 -->
                <el-card shadow="never" class="sub">
                  <template #header>
                    <div class="flex-between">
                      <b>表级 · {{ cur.name }}</b>
                      <div>
                        <el-button size="small" text type="warning" @click="openRowRule(cur)">行规则{{ cur.rowRule ? ' 🔒 ' + cur.rowRule : '' }}</el-button>
                        <el-text size="small" type="info" style="margin-left:8px">密级: {{ secName(cur.secret) }}</el-text>
                      </div>
                    </div>
                  </template>
                  <div class="flex" style="gap:8px;align-items:center">
                    <el-text size="small">当前主体能力档</el-text>
                    <el-select :model-value="cur.level" size="small" style="width:170px" @change="(v) => (cur.level = v)">
                      <el-option v-for="(lv, i) in levelNames" :key="i" :value="i"><span :class="'lv lv' + i">{{ lv }}</span></el-option>
                    </el-select>
                    <el-text size="small" type="info" style="margin-left:6px">{{ levelDesc(cur.level) }}</el-text>
                  </div>
                </el-card>

                <!-- 列级 -->
                <el-card shadow="never" class="sub" style="margin-top:10px">
                  <template #header><b>列级({{ subjectLabel }})</b> — 列存在与否(L0/L1)与可读方式(L2 脱敏 / L3 原值)</template>
                  <el-table :data="cur.columns" size="small" border>
                    <el-table-column prop="name" label="列" width="160" />
                    <el-table-column label="类型" width="90"><template #default="{ row }">{{ row.type }}</template></el-table-column>
                    <el-table-column label="敏感标记" width="90">
                      <template #default="{ row }"><el-tag v-if="row.sensitive" size="small" type="warning" effect="plain">敏感</el-tag><el-text v-else size="small" type="info">—</el-text></template>
                    </el-table-column>
                    <el-table-column label="能力档" width="220">
                      <template #default="{ row }">
                        <el-select :model-value="row.level" size="small" style="width:140px" @change="(v) => (row.level = v)">
                          <el-option v-for="(lv, i) in levelNames" :key="i" :value="i"><span :class="'lv lv' + i">{{ lv }}</span></el-option>
                        </el-select>
                        <el-tag v-if="row.level === 2" size="small" type="warning" effect="plain" style="margin-left:4px">脱敏: {{ row.mask }}</el-tag>
                      </template>
                    </el-table-column>
                    <el-table-column label="说明" min-width="180"><template #default="{ row }"><el-text size="small" type="info">{{ row.desc }}</el-text></template></el-table-column>
                  </el-table>
                </el-card>

                <!-- 可见视图预览 -->
                <el-card shadow="never" class="sub" style="margin-top:10px">
                  <template #header><b>该主体可见视图预览(演示)</b></template>
                  <el-text size="small" type="info">列: </el-text>
                  <el-tag v-for="c in visibleCols" :key="c.name" size="small" effect="plain" style="margin:2px">{{ c.name }}<span v-if="c.level === 2" class="dim">·{{ c.mask }}</span></el-tag>
                  <div style="margin-top:6px"><el-text size="small" type="info">行: </el-text><el-text size="small">{{ cur.rowRule ? '仅 ' + cur.rowRule : '全行' }}</el-text></div>
                </el-card>
              </template>
              <el-text v-else type="info">← 选择数据集</el-text>
            </div>
          </div>
        </el-tab-pane>

        <!-- ═══════════ T3 标签级 ═══════════ -->
        <el-tab-pane label="③ 标签级" name="tag">
          <el-table :data="tags" size="small" border>
            <el-table-column label="标签(注册树)" min-width="260">
              <template #default="{ row }">
                <span :style="{ paddingLeft: (row.depth || 0) * 18 + 'px', fontWeight: row.depth === 0 ? 600 : 400 }">
                  <el-icon style="vertical-align:-2px"><PriceTag /></el-icon>
                  {{ row.path }}
                </span>
              </template>
            </el-table-column>
            <el-table-column label="密级" width="80">
              <template #default="{ row }"><el-tag v-if="row.secret" size="small" effect="plain" :type="secType(row.secret)">{{ secName(row.secret) }}</el-tag></template>
            </el-table-column>
            <el-table-column label="能力档({{ subjectLabel }})" width="220">
              <template #default="{ row }">
                <template v-if="row.kind === 'tag'">
                  <el-select :model-value="row.level" size="small" style="width:150px" @change="(v) => (row.level = v)">
                    <el-option v-for="(lv, i) in levelNames" :key="i" :value="i"><span :class="'lv lv' + i">{{ lv }}</span></el-option>
                  </el-select>
                  <el-tag v-if="row.eff === 'deny'" size="small" type="danger" effect="plain" style="margin-left:4px">Deny</el-tag>
                </template>
                <el-text v-else size="small" type="info">作用于子标签(分类默认)</el-text>
              </template>
            </el-table-column>
            <el-table-column label="行规则(实体集)" width="170">
              <template #default="{ row }">
                <el-button v-if="row.kind === 'tag'" size="small" text :type="row.rowRule ? 'warning' : ''" @click="openRowRule(row)">
                  {{ row.rowRule ? '🔒 ' + row.rowRule : '＋ 配置' }}
                </el-button>
              </template>
            </el-table-column>
            <el-table-column label="能力语义" min-width="220">
              <template #default="{ row }">
                <template v-if="row.kind === 'tag'"><el-text size="small" type="info">{{ tagLevelDesc(row.level) }}</el-text></template>
                <el-text v-else size="small" type="info">业务域/风控 树节点默认档</el-text>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ═══════════ T4 数据订阅出口 ═══════════ -->
        <el-tab-pane label="④ 数据订阅出口" name="sub">
          <div class="sub-warn">
            <b>出口安全规则</b>:
            ① 订阅 = <b>订阅人实时权限快照</b>: 每次命中/投递按属主当前能力重算(标签 ≥L2 且行规则内), 属主降权 → 命中自动收缩并告警, 不做一次性授权;
            ② 圈选/订阅可取标签值与命中计数, <b>实体明细查看/导出要求 L3</b>(导出走审计);
            ③ Deny 优先; 密级 ≥ 机密的订阅/访问全审计; 订阅标签只能取 ≤ 属主能力的标签。
          </div>

          <el-table :data="subs" size="small" border style="margin-top:10px">
            <el-table-column label="订阅" min-width="170">
              <template #default="{ row }">
                <div style="font-weight:600">{{ row.name }}</div>
                <el-text size="small" type="info">#{{ row.id }} · 属主 {{ row.owner }} · 创建 {{ row.created }}</el-text>
              </template>
            </el-table-column>
            <el-table-column label="圈选条件" min-width="220">
              <template #default="{ row }">
                <el-tag size="small" type="primary" effect="plain">{{ row.tag }}</el-tag>
                <el-tag v-if="row.value" size="small" style="margin-left:4px">= {{ row.value }}</el-tag>
                <el-tag v-if="row.extraFilter" size="small" type="info" effect="plain" style="margin-left:4px">+ {{ row.extraFilter }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="投递" min-width="190">
              <template #default="{ row }">
                <div><el-tag size="small" effect="plain" :type="row.channel === '站内+Webhook' ? 'primary' : 'info'">{{ row.channel }}</el-tag></div>
                <el-text size="small" type="info">{{ row.freq }}</el-text>
                <div v-if="row.webhook" class="dim">{{ row.webhook }}</div>
              </template>
            </el-table-column>
            <el-table-column label="属主能力实时检查" min-width="170">
              <template #default="{ row }">
                <el-text size="small">标签档 <b :class="'lv lv' + row.ownerLevel">L{{ row.ownerLevel }}</b></el-text>
                <el-tag v-if="row.ownerLevel < 2" size="small" type="danger" effect="plain" style="margin-left:4px">不足 L2</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="投递统计(今日)" min-width="180">
              <template #default="{ row }">
                <div>命中 {{ row.hit }} · 投出 {{ row.pushed }}</div>
                <el-text v-if="row.filtered" size="small" type="warning">权限过滤 {{ row.filtered }} 条(见原因)</el-text>
                <div class="dim">最近投递 {{ row.lastAt }}</div>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="150">
              <template #default="{ row }">
                <el-tag size="small" :type="row.statusType" effect="plain">{{ row.status }}</el-tag>
                <div v-if="row.reason" class="dim" style="margin-top:2px">{{ row.reason }}</div>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="200">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="openSubDetail(row)">详情</el-button>
                <el-button size="small" text :disabled="row.ownerLevel < 3" @click="ElMessage.info('导出记录走审计(演示)')">导出明细</el-button>
                <el-button size="small" text type="danger" @click="row.statusType = 'info'; row.status = '已停用'">停用</el-button>
              </template>
            </el-table-column>
          </el-table>

          <!-- 出口审计 -->
          <el-card shadow="never" class="sub" style="margin-top:10px">
            <template #header><b>出口审计(最近投递/导出)</b></template>
            <el-table :data="audits" size="small">
              <el-table-column prop="ts" label="时间" width="170" />
              <el-table-column prop="who" label="主体" width="110" />
              <el-table-column prop="action" label="动作" width="130"><template #default="{ row }">{{ row.action }}</template></el-table-column>
              <el-table-column prop="target" label="对象" min-width="180" />
              <el-table-column prop="rows" label="行数" width="70" align="center" />
              <el-table-column label="结果" width="90"><template #default="{ row }"><el-tag size="small" :type="row.ok ? 'success' : 'danger'" effect="plain">{{ row.ok ? '放行' : '拒绝' }}</el-tag></template></el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <!-- 行规则弹层 -->
    <el-dialog v-model="ruleDlg" :title="'行规则 · ' + (ruleTarget ? ruleTarget.name : '')" width="620">
      <el-form label-width="90px" size="small">
        <el-form-item label="规则名"><el-input v-model="ruleForm.name" placeholder="如 仅华东 / 黑名单实体集" /></el-form-item>
        <el-form-item label="规则类型">
          <el-radio-group v-model="ruleForm.type">
            <el-radio label="cond">字段条件</el-radio>
            <el-radio label="list">实体清单(CSV)</el-radio>
            <el-radio label="tag">引用标签圈选</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item v-if="ruleForm.type === 'cond'" label="条件构造">
          <div class="cond-line">
            <el-select v-model="ruleForm.field" style="width:130px"><el-option v-for="c in ruleCols" :key="c" :label="c" :value="c" /></el-select>
            <el-select v-model="ruleForm.op" style="width:90px"><el-option v-for="o in ['=', '!=', 'in', '>']" :key="o" :label="o" :value="o" /></el-select>
            <el-input v-model="ruleForm.val" style="width:150px" placeholder="值/列表" />
            <el-button text type="primary" @click="ruleForm.preview = '命中实体约 ' + (10 + (ruleForm.val || '').length * 6) + ' 个(演示)'">预览</el-button>
          </div>
          <div v-if="ruleForm.preview" class="tip">{{ ruleForm.preview }}</div>
        </el-form-item>
        <el-form-item v-else-if="ruleForm.type === 'tag'" label="引用标签">
          <el-select v-model="ruleForm.tag" style="width:100%">
            <el-option v-for="t in ['业务域/风控/黑名单', '业务域/交易/订单/金额档']" :key="t" :label="t" :value="t" />
          </el-select>
          <el-text size="small" type="info" style="margin-left:8px">以该标签当前圈选实体为行范围(引用标签密级 ≤ 本资源)</el-text>
        </el-form-item>
        <el-form-item label="适用主体">
          <el-checkbox-group v-model="ruleForm.subjects">
            <el-checkbox v-for="s in ['ADMIN', 'LABEL_ENG', 'ANALYST', 'OPERATOR', 'VIEWER']" :key="s" :label="s" />
          </el-checkbox-group>
        </el-form-item>
      </el-form>
      <template #footer><el-button size="small" @click="ruleDlg = false">取消</el-button><el-button size="small" type="primary" @click="ruleDlg = false">保存(设计稿)</el-button></template>
    </el-dialog>

    <!-- 订阅详情弹层 -->
    <el-drawer v-model="subDlg" :title="'订阅 #' + (curSub ? curSub.id : '') + ' · ' + (curSub ? curSub.name : '')" size="560px">
      <template v-if="curSub">
        <el-descriptions :column="1" size="small" border>
          <el-descriptions-item label="圈选条件"><el-tag size="small" type="primary" effect="plain">{{ curSub.tag }}</el-tag><el-tag v-if="curSub.value" size="small" style="margin-left:4px">={{ curSub.value }}</el-tag></el-descriptions-item>
          <el-descriptions-item label="附加过滤">{{ curSub.extraFilter || '无' }}</el-descriptions-item>
          <el-descriptions-item label="投递通道">{{ curSub.channel }}<span v-if="curSub.webhook"> · {{ curSub.webhook }}</span></el-descriptions-item>
          <el-descriptions-item label="频率">{{ curSub.freq }}</el-descriptions-item>
          <el-descriptions-item label="属主能力">
            标签 L{{ curSub.ownerLevel }} <el-tag v-if="curSub.ownerLevel >= 3" size="small" type="success" effect="plain">可看明细</el-tag>
            <el-tag v-else-if="curSub.ownerLevel === 2" size="small" type="warning" effect="plain">仅计数/值, 不可下钻明细</el-tag>
            <el-tag v-else size="small" type="danger" effect="plain">不可圈选 → 订阅失效</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="生效范围">标签密级 ≤ {{ curSub.maxSecret }}; 行规则: {{ curSub.rowScope || '继承标签行规则' }}</el-descriptions-item>
        </el-descriptions>
        <div class="sec-title">今日投递明细(权限过滤原因透明展示)</div>
        <el-table :data="curSub.detail || []" size="small">
          <el-table-column prop="ts" label="时间" width="160" />
          <el-table-column prop="entity" label="实体" width="120" />
          <el-table-column prop="reason" label="结果" min-width="160"><template #default="{ row }"><el-text size="small" :type="row.reason === '已投递' ? 'success' : 'warning'">{{ row.reason }}</el-text></template></el-table-column>
        </el-table>
      </template>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { FolderOpened, Grid, PriceTag } from '@element-plus/icons-vue'

const tab = ref('scope')
const subjectKind = ref('role')
const roles = ['ADMIN', 'OPERATOR', 'LABEL_ENG', 'ANALYST', 'VIEWER']
const users = [
  { name: 'zhang', role: 'LABEL_ENG' }, { name: 'li', role: 'ANALYST' }, { name: 'wang', role: 'ANALYST' }, { name: 'chen', role: 'OPERATOR' }
]
const subject = ref('ANALYST')
const subjectLabel = computed(() => subject.value)
const subjectDefault = computed(() => ({ ADMIN: 'L3 全量', OPERATOR: 'L3 数据面 / L1 标签', LABEL_ENG: 'L3 打标域 / L2 消费', ANALYST: 'L2 默认', VIEWER: 'L1' })[subject.value] || '按角色默认')
const levelNames = ['L0 不可见', 'L1 结构可见', 'L2 脱敏可读', 'L3 完全可读']
const levelDesc = (i) => ['完全消失', '见结构不见数据', '脱敏/不下钻明细', '全量原值'][i]
const tagLevelDesc = (i) => ['目录不展示', '见定义, 不可圈选/订阅', '可圈选/订阅, 明细需 L3', '全能力+实体明细'][i]
const secName = (s) => ['', '公开', '内部', '机密', '绝密'][s]
const secType = (s) => ['', 'success', 'primary', 'warning', 'danger'][s]

/* T1 资源级 */
const scopeDim = ref('biz')
const scopeRows = computed(() => {
  if (scopeDim.value === 'biz') return bizRows
  if (scopeDim.value === 'sec') return secRows
  return srcRows
})
const bizRows = [
  { path: '业务域/风控', cover: '2 表 · 4 标签', secret: 3, level: 1, eff: 'explicit', desc: '风控域默认 L1: 见结构/定义, 不开放数据' },
  { path: '业务域/交易', cover: '3 表 · 5 标签', secret: 2, level: 2, eff: 'system', desc: '交易域默认 L2: 可圈选/订阅, 值脱敏' },
  { path: '业务域/客户', cover: '1 表 · 2 标签', secret: 2, level: 2, eff: 'system', desc: '客户域默认 L2' }
]
const secRows = [
  { path: '密级/公开', cover: '2 表', secret: 1, level: 3, eff: 'system', desc: '公开数据全员 L3' },
  { path: '密级/内部', cover: '5 表', secret: 2, level: 2, eff: 'system', desc: '内部默认 L2' },
  { path: '密级/机密', cover: '2 表', secret: 3, level: 1, eff: 'explicit', desc: '机密默认 L1, 单独授权才可读' }
]
const srcRows = [
  { path: '来源系统/OMS', cover: '3 表', secret: 2, level: 2, eff: 'system', desc: '交易系统源默认 L2' },
  { path: '来源系统/CRM', cover: '2 表', secret: 3, level: 1, eff: 'explicit', desc: 'CRM 源含客户敏感信息默认 L1' }
]

/* T2 表级 */
const curTable = ref('customer_wide')
const tables = [
  { name: 'customer_wide', secret: 2, level: 1, rowRule: 'region=华东', columns: [
    { name: 'id', type: 'bigint', level: 3, sensitive: false, desc: '' },
    { name: 'customer_id', type: 'varchar', level: 3, sensitive: true, desc: '业务键, L3' },
    { name: 'name', type: 'varchar', level: 1, sensitive: true, desc: '姓名仅结构可见' },
    { name: 'region', type: 'varchar', level: 3, sensitive: false, desc: '' },
    { name: 'amount', type: 'decimal', level: 2, sensitive: true, mask: '≥1000→大额档', desc: '金额 L2: 区间桶替代原值' },
    { name: 'status', type: 'varchar', level: 3, sensitive: false, desc: '' }
  ] },
  { name: 't_order_model', secret: 2, level: 0, rowRule: '', columns: [] },
  { name: 't_mix_poc', secret: 2, level: 2, rowRule: '', columns: [] }
]
const cur = computed(() => tables.find(t => t.name === curTable.value))
const visibleCols = computed(() => (cur.value?.columns || []).filter(c => c.level >= 1))

/* T3 标签 */
const tags = [
  { kind: 'group', path: '业务域/风控', depth: 0, secret: 3, desc: '' },
  { kind: 'tag', path: '业务域/风控/黑名单', depth: 1, secret: 3, level: 1, eff: 'explicit', rowRule: '', desc: '' },
  { kind: 'group', path: '业务域/交易/订单', depth: 0, secret: 2 },
  { kind: 'tag', path: '业务域/交易/订单/金额档', depth: 1, secret: 2, level: 2, eff: 'explicit', rowRule: '金额档=大额/中额' },
  { kind: 'tag', path: '业务域/交易/订单/净支付额', depth: 1, secret: 2, level: 2, eff: 'inherit', rowRule: '' }
]

/* T4 订阅出口 */
const subs = [
  { id: 1, name: '黑名单监控(实时)', owner: 'zhang', created: '09-08', tag: '业务域/风控/黑名单', value: '', extraFilter: '', channel: '站内+Webhook', webhook: 'https://alert.dcp.local/hook/black', freq: '实时(事件触发)', ownerLevel: 3, hit: 30, pushed: 28, filtered: 2, reason: '2 条实体属风控行规则外', status: '运行中', statusType: 'success', lastAt: '21:40', maxSecret: 3, detail: [
    { ts: '21:40', entity: 'E9001', reason: '已投递' }, { ts: '21:36', entity: 'C8802', reason: '已投递' }, { ts: '21:33', entity: 'C8802', reason: '权限过滤: 属主行规则外' }
  ] },
  { id: 2, name: '大额圈选(日汇总)', owner: 'li', created: '09-07', tag: '业务域/交易/订单/金额档', value: '大额', extraFilter: 'amount≥5000', channel: '站内', freq: '每日 09:00 汇总', ownerLevel: 2, hit: 12, pushed: 9, filtered: 3, reason: '3 条实体仅 L3 可见(金额档行规则外实体)', status: '运行中(部分过滤)', statusType: 'warning', lastAt: '09:00', maxSecret: 2, detail: [
    { ts: '09:00', entity: 'E1201', reason: '已投递' }, { ts: '09:00', entity: 'E2200', reason: '已投递' }, { ts: '09:00', entity: 'C1100', reason: '权限过滤: 需 L3 明细' }
  ] },
  { id: 3, name: '净支付额观察', owner: 'wang', created: '08-30', tag: '业务域/交易/订单/净支付额', value: '大额', extraFilter: '', channel: '站内', freq: '实时', ownerLevel: 1, hit: 41, pushed: 0, filtered: 41, reason: '属主能力 L3→L1: 标签不可圈选, 已自动停推', status: '⚠ 属主降权·已停推', statusType: 'danger', lastAt: '—', maxSecret: 2, detail: [] }
]
const audits = [
  { ts: '2026-09-08 21:40', who: 'zhang', action: '订阅投递', target: '订阅#1 黑名单监控 → webhook', rows: 2, ok: true },
  { ts: '2026-09-08 21:33', who: 'wang', action: '订阅命中重算', target: '订阅#3 净支付额观察', rows: 41, ok: false },
  { ts: '2026-09-08 18:22', who: 'li', action: '导出明细(L3)', target: '订阅#2 圈选实体', rows: 12, ok: true },
  { ts: '2026-09-08 18:20', who: 'li', action: '导出明细(L3)', target: '订阅#2 圈选实体', rows: 0, ok: true }
]

/* 行规则/订阅弹层 */
const ruleDlg = ref(false)
const ruleTarget = ref(null)
const ruleForm = ref({ name: '', type: 'cond', field: '', op: '=', val: '', tag: '', subjects: ['ANALYST'], preview: '' })
const ruleCols = ['id', 'customer_id', 'name', 'region', 'status', 'amount']
function openRowRule(row) {
  ruleTarget.value = row
  ruleForm.value = { name: row.rowRule || '', type: 'cond', field: '', op: '=', val: '', tag: '', subjects: [subject.value], preview: '' }
  ruleDlg.value = true
}
const subDlg = ref(false)
const curSub = ref(null)
function openSubDetail(row) {
  curSub.value = row
  subDlg.value = true
}
</script>

<style scoped>
.page-card { max-width: 1560px; }
.pg-title { font-size: 17px; font-weight: 700; }
.head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; flex-wrap: wrap; }
.subject-bar { display: flex; align-items: center; gap: 6px; }
.lbl { font-weight: 600; font-size: 13px; }
.flex { display: flex; }
.flex-between { display: flex; justify-content: space-between; align-items: center; }
.trow { display: flex; align-items: center; gap: 6px; padding: 7px 10px; cursor: pointer; border-radius: 6px; font-size: 13px; }
.trow:hover { background: var(--el-fill-color-light); }
.trow.on { background: var(--el-color-primary-light-9); font-weight: 600; }
.side :deep(.el-card__body) { padding: 8px; }
.sub { border: none; }
.sec-title { font-size: 13px; font-weight: 600; margin: 12px 0 6px; }
.panel-tip { background: #f0f7ff; border: 1px solid #c6e0f7; color: #2a5d8f; padding: 8px 10px; border-radius: 6px; font-size: 12px; }
.sub-warn { background: #fdf6ec; border: 1px solid #f3d19e; color: #7a5b1f; padding: 10px 12px; border-radius: 6px; font-size: 13px; line-height: 1.8; }
.dim { color: #999; font-size: 12px; }
.lv { font-weight: 600; }
.lv0 { color: #909399; } .lv1 { color: #409eff; } .lv2 { color: #e6a23c; } .lv3 { color: #67c23a; }
.cond-line { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
</style>

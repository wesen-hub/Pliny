<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-steps :active="step" align-center finish-status="success" style="margin-bottom: 24px">
        <el-step title="源表配置" description="数据源 + 源表 + 采集方式" />
        <el-step title="业务主键配置" description="键模式 + 字段组合" />
        <el-step title="模型字段映射" description="每字段 = 来源 + 加工链" />
        <el-step title="行级转换" description="Filter / FilterRowKind / 跨表" />
        <el-step title="调度与提交" description="批量间隔 + 审批" />
      </el-steps>

      <!-- Step 1 源表配置 -->
      <div v-show="step === 0">
        <div class="sec-title">① 选择数据源(来自数据源管理)</div>
        <el-select v-model="form.ds" style="width: 320px" placeholder="选择数据源" @change="onDsChange">
          <el-option v-for="d in datasources" :key="d.id" :label="`${d.name} (${d.type})`" :value="d.id" />
        </el-select>
        <el-text v-if="!form.ds" type="info" size="small" style="margin-left: 12px">数据源在「数据源管理」中接入,此处直接选用</el-text>

        <template v-if="form.ds">
          <div class="sec-title mt16">② 选择{{ dbLabel }}({{ dbLabel }}层)</div>
          <el-select v-if="catalogOptions.length" v-model="form.db" style="width: 260px" placeholder="选择数据库" @change="onDbChange">
            <el-option v-for="c in catalogOptions" :key="c" :value="c" :label="c" />
          </el-select>
          <el-text v-else-if="dbType === 'oracle'" type="info" size="small">Oracle 无 database 层,直接选择 Schema(用户)</el-text>

          <template v-if="schemaOptions.length">
            <div class="sec-title mt16">③ 选择 Schema</div>
            <el-select v-model="form.schema" style="width: 260px" placeholder="选择 Schema" @change="onSchemaChange">
              <el-option v-for="s in schemaOptions" :key="s" :value="s" :label="s" />
            </el-select>
          </template>

          <div class="sec-title mt16">④ 选择源{{ tableLabel }}(可多选,支持多源{{ tableLabel }}合并到同一模型)</div>
          <el-checkbox-group v-model="form.tables">
            <el-checkbox v-for="t in dsTables" :key="t.name" :value="t.name" :label="t.name" border style="margin: 0 10px 10px 0">{{ t.name }}</el-checkbox>
          </el-checkbox-group>

          <div class="sec-title mt16">更多来源(可选): 每个来源独立选择数据源/库, 多源表合并到同一模型</div>
          <el-alert v-if="tableDup.length" type="warning" :closable="false" show-icon style="margin-top:10px"
            :title="`源表名冲突(不同库/源同名): ${tableDup.join(', ')} — 映射按表名区分, 请改名或拆成两个任务`" />
          <div v-for="(g, gi) in extraGroups" :key="gi" class="eg-card">
            <div class="flex-between"><b>来源 {{ gi + 1 }}</b>
              <el-link type="danger" underline="never" @click="removeExtra(gi)">移除</el-link></div>
            <div class="sec-title" style="font-weight:500;margin:8px 0 6px">数据源</div>
            <el-select v-model="g.ds" style="width:280px" placeholder="选择数据源" @change="onEgDsChange(gi)">
              <el-option v-for="d in datasources" :key="d.id" :label="`${d.name} (${d.type})`" :value="d.id" />
            </el-select>
            <div class="sec-title" style="font-weight:500;margin:8px 0 6px">数据库 / Schema</div>
            <el-select v-if="g.dbs.length" v-model="g.db" style="width:220px" placeholder="选择数据库" @change="onEgDbChange(gi)">
              <el-option v-for="c in g.dbs" :key="c" :value="c" :label="c" />
            </el-select>
            <el-text v-else-if="g.ds" type="info" size="small">加载数据库中…</el-text>
            <div class="sec-title" style="font-weight:500;margin:8px 0 6px">选择源表(可多选)</div>
            <el-checkbox-group v-model="g.selected">
              <el-checkbox v-for="t in g.tables" :key="t.name" :value="t.name" :label="t.name" border style="margin: 0 10px 8px 0">{{ t.name }}</el-checkbox>
            </el-checkbox-group>
            <el-text v-if="g.ds && g.db && !g.tables.length" type="info" size="small">该库暂无表或加载中</el-text>
          </div>
          <el-button size="small" style="margin-top:10px" type="primary" plain @click="addExtra">＋ 新增数据源来源</el-button>

          <div v-if="form.tables.length" class="mt16">
            <div class="sec-title" style="display:none">⑤ 采集方式与源表主键(每表)<!-- 已废弃: 模式为任务级 --></div>
            <div class="sec-title mt16">⑤ 同步模式(任务级, 本任务全部源表共享)<el-text type="info" size="small">全链路走 SeaTunnel CDC → Kafka → 收敛落库</el-text></div>
            <el-form label-width="110px" style="max-width: 620px">
              <el-form-item label="同步模式" style="display:block">
                <div class="sync-mode-label" style="font-weight:600;margin-bottom:6px">同步模式</div>
                <el-radio-group v-model="form.syncMode" class="mode-cards">
                  <el-radio value="full-cdc" border class="mode-card">
                    <b>全量+CDC</b><span class="sub">快照存量 + 持续增量(常驻)</span>
                  </el-radio>
                  <el-radio value="incremental" border class="mode-card">
                    <b>仅增量</b><span class="sub">实时 CDC, 不做全量快照</span>
                  </el-radio>
                  <el-radio value="full-once" border class="mode-card">
                    <b>一次性全量</b><span class="sub">同步存量后自动结束</span>
                  </el-radio>
                  <el-radio value="full-scheduled" border class="mode-card">
                    <b>全量+定时批量</b><span class="sub">首次全量打底, cron 从上次位点增量批</span>
                  </el-radio>
                  <el-radio value="scheduled-batch" border class="mode-card">
                    <b>定时批量(纯增量)</b><span class="sub">cron 触发, 仅增量不跑全量</span>
                  </el-radio>
                </el-radio-group>
              </el-form-item>
              <el-form-item v-if="form.syncMode === 'full-once'" label="全量范围">
                <el-radio-group v-model="form.fullScope">
                  <el-radio value="snapshot-only">纯快照(不含运行中增量)</el-radio>
                  <el-radio value="snapshot-incr">快照+运行中增量</el-radio>
                </el-radio-group>
              </el-form-item>
              <el-form-item v-if="form.syncMode === 'scheduled-batch' || form.syncMode === 'full-scheduled'" label="执行时间(cron)">
                <el-input v-model="form.cronExpr" placeholder="如: 0 30 2 * * * (每天02:30)" style="width: 280px" />
                <div class="sub">每轮从上次执行的位点续跑, 追上当前即停</div>
              </el-form-item>
              <el-form-item v-if="form.syncMode === 'incremental' || form.syncMode === 'scheduled-batch'" label="起始位点">
                <div style="width:100%">
                  <el-radio-group v-model="form.startMode">
                    <el-radio value="earliest">最早可用位点(默认, 从源头最早开始补)</el-radio>
                    <el-radio value="latest">最新位点(只收启动后的新变更)</el-radio>
                    <el-radio value="timestamp">指定时间点</el-radio>
                    <el-radio value="specific-offsets">自定义 binlog 位点(MySQL)</el-radio>
                  </el-radio-group>
                  <div v-if="form.startMode === 'timestamp'" style="margin-top:8px">
                    <el-date-picker v-model="form.startTime" type="datetime" placeholder="从该时刻开始的变更"
                      value-format="YYYY-MM-DD HH:mm:ss" style="width: 260px" @change="onStartTime" />
                  </div>
                  <div v-if="form.startMode === 'specific-offsets'" style="margin-top:8px">
                    <el-input v-model="form.startValue" placeholder="binlog file:pos, 如 mysql-bin.000001:4" style="width: 280px" />
                  </div>
                  <div class="sub">timestamp = 从指定时刻起的增量(epoch ms); specific = MySQL binlog 精确坐标</div>
                </div>
              </el-form-item>
            </el-form>
            <div class="sec-title" style="font-weight:400">源表主键(全量分块/断点续传用; 目标侧收敛键在下一步)<el-text type="info" size="small">源表主键 = 全量分块/断点续传用(源侧); 目标侧路由/收敛的键在下一步「业务主键配置」, 两者可不同</el-text></div>
            <el-table :data="tableConfigs" stripe size="small">
              <el-table-column prop="table" label="源表" width="160" />
              <el-table-column label="源表主键" width="160">
                <template #default="{ row }">
                  <el-select v-model="row.pk" size="small" style="width: 130px"
                    :placeholder="'全量分块/续传用'"
                    :title="'源表物理主键: 全量分块与断点续传依据(源侧); 目标侧路由/收敛用「业务主键」, 下一步配置, 两者可不同'">
                    <el-option v-for="c in row.pkCandidates" :key="c" :label="c" :value="c" />
                  </el-select>
                </template>
              </el-table-column>
              <el-table-column label="源主键(分块键)" width="200">
                <template #default="{ row }"><span class="mono">来源系统 = {{ row.system }} (hash % 8)</span></template>
              </el-table-column>
              <el-table-column label="预估行数" prop="rows" width="100" />
            </el-table>
          </div>
        </template>

        <div class="sec-title mt16">⑥ 目标模型(已发布)</div>
        <el-select v-model="form.model" style="width: 320px" placeholder="选择目标模型" @change="onModelChange">
          <el-option v-for="m in publishedModels" :key="m.id" :label="`${m.name} (${m.layer})`" :value="m.id" />
        </el-select>
        <el-text v-if="form.model" type="success" size="small" style="margin-left: 12px">✔ 模型已发布,可开始映射</el-text>
      </div>

      <!-- Step 2 业务主键配置 -->
      <div v-show="step === 1">
        <el-alert v-if="form.tables.length > 1" type="warning" :closable="false" show-icon style="margin-bottom: 14px"
          title="多表合并模式(字段级收敛): 多张源表 → 同一张宽表"
          description="约束: ① 业务主键选 merge 模式且各源表键一致; ② 键不能含物理身份/表名; ③ 各源表字段尽量不重叠(同字段冲突由全局 seq 裁判); ④ 宽表列 = 各源表字段全集, 自动建表/补列" />
        <BusinessKeyConfig v-model="form.businessKey" :fields="modelFields" :db-type="dbType" :system-options="systemOptions" />
      </div>

      <!-- Step 3 模型字段映射(字段处理链) -->
      <div v-show="step === 2">
        <el-alert type="info" :closable="false" show-icon style="margin-bottom: 14px"
          title="每个模型字段 = 来源(互斥,仅一个: 字段映射/计算字段/UUID/系统生成) + 加工链(可叠加按序: 空值填充→拆分→大小写→截断→脱敏→JSON解析→日期处理→…);点击行展开编辑" />
        <el-alert v-if="!undefinedFields.length && !badRefs.length && !typeWarnings.length" type="success" :closable="false" show-icon style="margin-bottom: 12px"
          :title="`模型字段 ${fieldMapping.length} 个全部已定义 ✓ 引用校验通过 ✓ 类型对拍通过 ✓`" />
        <el-alert v-else type="warning" :closable="false" show-icon style="margin-bottom: 12px" title="存在未定义或校验问题,需处理后才能提交">
          <template #default>
            <div v-if="undefinedFields.length">⚠ 未定义来源: {{ undefinedFields.join(', ') }}(已定义 {{ fieldMapping.length - undefinedFields.length }}/{{ fieldMapping.length }})</div>
            <div v-for="b in badRefs" :key="b.modelField">⚠ 字段 <b>{{ b.modelField }}</b> 表达式引用了未知字段: {{ b.unknown.join(', ') }}</div>
            <div v-for="t in typeWarnings" :key="t">⚠ 字段 <b>{{ t }}</b> 处理结果类型与模型字段类型不匹配</div>
          </template>
        </el-alert>
        <el-table :data="fieldMapping" stripe size="small" row-key="modelField" :row-class-name="rowClass">
          <el-table-column type="expand">
            <template #default="{ row }">
              <div class="field-chain">
                <!-- 来源(互斥) -->
                <div class="fc-section">
                  <div class="fc-label">来源(互斥,仅一个)<el-text type="info" size="small">切换来源类型即替换,计算字段与字段映射天然互斥</el-text></div>
                  <div class="fc-row">
                    <el-select v-model="row.source.kind" size="small" style="width: 170px" @change="onSourceChange(row)">
                      <el-option v-for="s in sourceKinds" :key="s.id" :label="s.name" :value="s.id" />
                    </el-select>
                    <template v-if="row.source.kind === 'field-map'">
                      <el-select v-model="row.source.srcTable" size="small" style="width: 120px" @change="row.source.srcField = ''">
                        <el-option v-for="t in allTables" :key="t" :label="t" :value="t" />
                      </el-select>
                      <el-select v-model="row.source.srcField" size="small" style="width: 130px" placeholder="源字段">
                        <el-option v-for="f in srcFieldList(row.source.srcTable)" :key="f" :label="f" :value="f" />
                      </el-select>
                    </template>
                    <template v-else-if="row.source.kind === 'compute'">
                      <el-input v-model="row.source.expr" size="small" style="width: 300px" placeholder="Aviator 表达式,如 amount * (1 + tax_rate)" />
                    </template>
                    <template v-else-if="row.source.kind === 'uuid'">
                      <el-select v-model="row.source.strategy" size="small" style="width: 200px">
                        <el-option label="UUID v4(随机,默认)" value="v4" />
                        <el-option label="UUID v7(时间有序,索引友好)" value="v7" />
                        <el-option label="雪花 ID(趋势递增,64位)" value="snowflake" />
                      </el-select>
                      <el-text type="info" size="small" style="margin-left:8px">{{ uuidTip(row.source.strategy) }}</el-text>
                    </template>
                    <el-text v-else-if="row.source.kind === 'system'" type="info" size="small">系统生成(全局序号 seq / 时间戳)</el-text>
                  </div>
                  <div v-if="row.source.kind === 'compute'" class="field-chips" style="margin-top: 6px">
                    <el-tag v-for="f in allRefFields" :key="f" size="small" class="chip" @click="row.source.expr += ' ' + f">{{ f }}</el-tag>
                  </div>
                </div>
                <!-- 加工链 -->
                <div class="fc-section">
                  <div class="fc-label">加工链(可叠加,按序执行)
                    <el-button link type="primary" size="small" style="margin-left: 8px" @click="openStep(row)">＋ 添加加工</el-button>
                  </div>
                  <div class="fc-steps">
                    <template v-if="!row.steps.length"><el-text type="info" size="small">无加工,直接使用来源值</el-text></template>
                    <template v-for="(s, i) in row.steps" :key="i">
                      <div class="fc-step">
                        <el-tag size="small" :type="stepTypeColor(s.kind)" effect="plain">{{ stepName(s.kind) }}</el-tag>
                        <span class="fc-summary mono">{{ stepSummary(s) }}</span>
                        <span class="fc-ops">
                          <el-link type="primary" underline="never" size="small" @click="editStep(row, s)">编辑</el-link>
                          <el-link type="primary" underline="never" size="small" :disabled="i === 0" @click="moveStep(row, s, -1)">上移</el-link>
                          <el-link type="primary" underline="never" size="small" :disabled="i === row.steps.length - 1" @click="moveStep(row, s, 1)">下移</el-link>
                          <el-link type="danger" underline="never" size="small" @click="removeStep(row, s)">删除</el-link>
                        </span>
                      </div>
                      <div v-if="i < row.steps.length - 1" class="fc-arrow">↓</div>
                    </template>
                  </div>
                </div>
              </div>
            </template>
          </el-table-column>
          <el-table-column prop="modelField" label="模型字段" width="120">
            <template #default="{ row }"><b>{{ row.modelField }}</b></template>
          </el-table-column>
          <el-table-column prop="type" label="类型" width="120">
            <template #default="{ row }"><span class="mono">{{ row.type }}</span></template>
          </el-table-column>
          <el-table-column label="来源与加工链" min-width="300">
            <template #default="{ row }">
              <span class="mono">{{ sourceSummary(row) }}</span>
              <template v-if="row.steps.length">
                <el-icon style="color:#c0c4cc;margin:0 4px"><Right /></el-icon>
                <el-tag v-for="s in row.steps" :key="s.kind + row.steps.indexOf(s)" size="small" style="margin-right: 4px">{{ stepName(s.kind) }}</el-tag>
              </template>
            </template>
          </el-table-column>
          <el-table-column label="结果预览" width="140">
            <template #default="{ row }"><span class="mono preview">{{ previews[row.modelField] ?? '—' }}</span></template>
          </el-table-column>
          <el-table-column label="状态" width="90">
            <template #default="{ row }">
              <el-tag size="small" :type="row.source.kind ? (isBad(row) ? 'danger' : 'success') : 'warning'" effect="plain">
                {{ row.source.kind ? (isBad(row) ? '检查' : '已定义') : '未定义' }}
              </el-tag>
            </template>
          </el-table-column>
        </el-table>
        <div class="tip">冲突规则: ①来源互斥(字段映射/计算字段/UUID/系统生成 只能选一个,切换即替换) ②加工类可叠加且顺序敏感 ③表达式可引用 源字段 + 前置模型字段(拓扑序) ④结果类型与模型字段类型对拍</div>
      </div>

      <!-- Step 4 行级转换 -->
      <div v-show="step === 3">
        <el-alert type="info" :closable="false" show-icon style="margin-bottom: 14px"
          title="行级转换(采集端执行, 仅两种): Filter 按条件保留/丢弃 + FilterRowKind 按操作类型过滤;字段级处理(映射/空值/计算/脱敏/JSON/日期…)已在步骤③的字段处理链中定义" />
        <el-row :gutter="16">
          <el-col :span="5">
            <div class="sec-title">输入源表</div>
            <div v-for="t in tableConfigs" :key="t.table" class="tbl-item" :class="{ active: t.table === curTable }" @click="curTable = t.table">
              <div class="tbl-name">{{ t.table }}</div>
              <div class="tbl-meta">{{ chainOf(t.table).filter(n => n.enabled).length }} 个启用节点</div>
            </div>
            <div class="sec-title mt16">输出</div>
            <div class="tbl-item">
              <div class="tbl-name">{{ modelName }}</div>
              <div class="tbl-meta">{{ modelLayer }} · 目标模型</div>
            </div>
          </el-col>
          <el-col :span="19">
            <div class="toolbar">
              <div class="sec-title" style="margin: 0">行级转换链: {{ curTable }} <el-text type="info" size="small">(按序执行)</el-text></div>
              <el-button size="small" type="primary" @click="openNode">＋ 添加行级节点</el-button>
            </div>
            <div class="chain">
              <div class="chain-input">输入: {{ curTable }}</div>
              <template v-for="(node, idx) in chainOf(curTable)" :key="node.id">
                <div class="chain-arrow">↓</div>
                <div class="chain-node" :class="{ disabled: !node.enabled }">
                  <div class="cn-main">
                    <el-tag size="small" :type="nodeTypeColor(node.type)" effect="plain">{{ node.type }}</el-tag>
                    <span class="cn-name">{{ node.name }}</span>
                    <span class="cn-summary mono">{{ makeSummary(node) }}</span>
                  </div>
                  <div class="cn-ops">
                    <el-link type="primary" underline="never" size="small" @click="editNode(node)">编辑</el-link>
                    <el-link underline="never" size="small" @click="toggleNode(node)">{{ node.enabled ? '停用' : '启用' }}</el-link>
                    <el-link type="primary" underline="never" size="small" :disabled="idx === 0" @click="moveNode(node, -1)">上移</el-link>
                    <el-link type="primary" underline="never" size="small" :disabled="idx === chainOf(curTable).length - 1" @click="moveNode(node, 1)">下移</el-link>
                    <el-link type="danger" underline="never" size="small" @click="removeNode(node)">删除</el-link>
                  </div>
                </div>
              </template>
              <div v-if="chainOf(curTable).length" class="chain-arrow">↓</div>
              <div class="chain-output">输出: {{ modelName }} ({{ modelLayer }})</div>
            </div>
            <div class="tip">能力(采集端执行): Filter(条件保留/丢弃) / FilterRowKind(按 INSERT/UPDATE/DELETE);过滤掉的行不进 Kafka;去重由目标侧主键+seq 天然保证, 跨表合并见多源表合并</div>
          </el-col>
        </el-row>
      </div>

      <!-- Step 5 调度与提交 -->
      <div v-show="step === 4">
        <div class="sec-title">调度与确认</div>
        <el-descriptions :column="1" border style="max-width: 640px">
          <el-descriptions-item label="数据源">{{ dsName }}</el-descriptions-item>
          <el-descriptions-item label="源表">{{ allTables.join(' + ') }}</el-descriptions-item>
          <el-descriptions-item label="来源组">{{ dsSummary }}</el-descriptions-item>
          <el-descriptions-item label="目标模型">{{ modelName }} ({{ modelLayer }})</el-descriptions-item>
          <el-descriptions-item label="业务主键">{{ bkSummary }}</el-descriptions-item>
          <el-descriptions-item label="字段映射">{{ definedCount }}/{{ fieldMapping.length }} 已定义<el-text v-if="undefinedFields.length" type="danger" size="small">(缺: {{ undefinedFields.join(', ') }})</el-text></el-descriptions-item>
          <el-descriptions-item label="加工步骤总数">{{ totalSteps }} 个(字段处理链)</el-descriptions-item>
          <el-descriptions-item label="行级节点">{{ totalChain }} 个(启用)</el-descriptions-item>
          <el-descriptions-item label="批量间隔">{{ form.interval }} min(湖可见延迟 ≤ 15min)</el-descriptions-item>
        </el-descriptions>
        <el-checkbox v-model="asNew" style="margin: 6px 0 0 2px"
                     :disabled="canNext !== true && false">
          <b>另存为独立新任务实例</b>
          <el-text type="info" size="small">勾选后, 即使该逻辑表已有任务, 也新建一套采集实例(各自独立 seq/写者列, 新实例 seq 从 1 起, 后写覆盖先写)。替换升级请: 先停旧实例 → 勾选提交 → 启动新实例</el-text>
        </el-checkbox>
        <div class="tip">提交后进入审批流;审批通过自动生成: 采集任务(SeaTunnel CDC → Kafka) + 写任务(TiDB 单表收敛) + 湖任务(Iceberg MERGE, 规划) + 血缘链路(按字段处理链推导)</div>
      </div>

      <div class="flex-between" style="margin-top: 20px">
        <el-button :disabled="step === 0" @click="step--">上一步</el-button>
        <div>
          <el-button v-if="step < 4" type="primary" :disabled="!canNext" @click="step++">下一步</el-button>
          <el-button v-else type="primary" :disabled="hasIssue" @click="submit">提交审批</el-button>
        </div>
      </div>
    </el-card>

    <!-- 加工步骤参数 -->
    <el-dialog v-model="stepDialog" :title="editingStep ? '编辑加工步骤' : '添加加工步骤'" width="520px">
      <el-form label-width="90px">
        <el-form-item label="加工类型">
          <el-select v-model="stepForm.kind" style="width: 100%" @change="onStepKindChange">
            <el-option v-for="k in stepKinds" :key="k.id" :label="`${k.name} — ${k.desc}`" :value="k.id" />
          </el-select>
        </el-form-item>
        <template v-if="stepForm.kind === 'null-fill'">
          <el-form-item label="填充值"><el-input v-model="stepForm.params.fillValue" placeholder="空值时填充的内容,如: 未知 / 0" /></el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'case'">
          <el-form-item label="转换方向">
            <el-radio-group v-model="stepForm.params.caseType"><el-radio value="upper">大写</el-radio><el-radio value="lower">小写</el-radio></el-radio-group>
          </el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'truncate'">
          <el-form-item label="最大长度"><el-input-number v-model="stepForm.params.length" :min="1" :max="255" /></el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'mask'">
          <el-form-item label="脱敏类型">
            <el-select v-model="stepForm.params.maskType" style="width: 100%">
              <el-option label="手机号(138****2210)" value="phone" /><el-option label="身份证" value="idcard" />
              <el-option label="金额" value="amount" /><el-option label="自定义掩码" value="custom" />
            </el-select>
          </el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'json-parse'">
          <el-form-item label="JSON 路径"><el-input v-model="stepForm.params.path" placeholder="如 $.name 或 $.items[0].id" /></el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'date'">
          <el-form-item label="输出格式"><el-input v-model="stepForm.params.format" placeholder="如 %Y-%m-%d %H:%i:%s" /></el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'split'">
          <el-form-item label="分隔符"><el-input v-model="stepForm.params.sep" placeholder="如 ," /></el-form-item>
          <el-form-item label="新字段名"><el-input v-model="stepForm.params.newFields" placeholder="如 region, city(拆分后生成新字段)" /></el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'cast'">
          <el-form-item label="目标类型">
            <el-select v-model="stepForm.params.targetType" style="width: 100%">
              <el-option v-for="x in ['BIGINT','INT','DECIMAL(18,2)','VARCHAR(64)','TIMESTAMP','DATE']" :key="x" :label="x" :value="x" />
            </el-select>
          </el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'replace'">
          <el-form-item label="查找"><el-input v-model="stepForm.params.find" /></el-form-item>
          <el-form-item label="替换为"><el-input v-model="stepForm.params.to" /></el-form-item>
        </template>
        <template v-else-if="stepForm.kind === 'enum'">
          <el-form-item label="枚举字典"><el-input v-model="stepForm.params.dict" placeholder="如 1→已支付, 2→已完成" /></el-form-item>
        </template>
      </el-form>
      <template #footer>
        <el-button @click="stepDialog = false">取消</el-button>
        <el-button type="primary" :disabled="!stepForm.kind" @click="confirmStep">确定</el-button>
      </template>
    </el-dialog>

    <!-- 行级节点参数 -->
    <el-dialog v-model="nodeDialog" :title="editingNode ? '编辑行级节点' : '添加行级节点'" width="600px">
      <el-form label-width="90px">
        <el-form-item label="能力类型">
          <el-select v-model="nodeForm.type" style="width: 100%" @change="onTypeChange">
            <el-option-group v-for="g in transformTypes" :key="g.group" :label="g.group">
              <el-option v-for="t in g.types" :key="t.id" :label="`${t.name} — ${t.desc}`" :value="t.id" />
            </el-option-group>
          </el-select>
        </el-form-item>
        <el-form-item label="节点名称"><el-input v-model="nodeForm.name" placeholder="如: 过滤逻辑删除" /></el-form-item>
        <template v-if="nodeForm.type === 'filter'">
          <el-form-item label="过滤条件"><el-input v-model="nodeForm.params.expr" type="textarea" :rows="2" placeholder="如: is_deleted != 1 && amount >= 0 (Aviator 语法, 用 && / || )" /></el-form-item>
          <el-form-item label="过滤模式">
            <el-radio-group v-model="nodeForm.params.mode"><el-radio value="keep">保留匹配</el-radio><el-radio value="drop">丢弃匹配</el-radio></el-radio-group>
          </el-form-item>
        </template>
        <template v-else-if="nodeForm.type === 'filter-kind'">
          <el-form-item label="保留操作类型">
            <el-checkbox-group v-model="nodeForm.params.kinds">
              <el-checkbox value="INSERT" label="INSERT" /><el-checkbox value="UPDATE" label="UPDATE" /><el-checkbox value="DELETE" label="DELETE" />
            </el-checkbox-group>
          </el-form-item>
        </template>
      </el-form>
      <template #footer>
        <el-button @click="nodeDialog = false">取消</el-button>
        <el-button type="primary" :disabled="!nodeForm.type" @click="confirmNode">{{ editingNode ? '保存' : '添加节点' }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { listModels } from '../../api'
import { ElMessage } from 'element-plus'
import BusinessKeyConfig from './components/BusinessKeyConfig.vue'
import { dbLevels } from '../../utils/dbLevels'
import { listDatasources, datasourceDatabases, datasourceSchemas, datasourceDbTables,
         datasourceDbColumns, saveLogicalTable, saveSyncTask } from '../../api'
const router = useRouter()
const step = ref(0)
const asNew = ref(false) // 另存为独立新任务实例(同(逻辑,源)与旧任务并存, 需先停旧实例)
const form = ref({ ds: '', db: '', schema: '', tables: [], model: '', syncMode: 'full-cdc', fullScope: 'snapshot-incr', cronExpr: '', startMode: 'earliest', startValue: '', startTime: '', businessKey: { mode: 'merge', scopeFields: ['system_name'], systemNames: [], keyFields: [], confirmPhysical: false } })
const curTable = ref('')

// ---------- 数据源 / 模型(真实 API: 数据源 → 库 → Schema → 表 级联) ----------
const datasources = ref([])
const loadingDs = ref(false)
const dsDatabases = ref([])
const dsSchemas = ref([])
const dsTableObjs = ref([])          // [{name, type}]
const columnsCache = ref({})         // 表名 → 字段列表(主键标记)

async function loadDatasources() {
  loadingDs.value = true
  try {
    const data = await listDatasources({ page: 1, pageSize: 200 })
    datasources.value = data.list || []
  } finally {
    loadingDs.value = false
  }
}
const route = useRoute()
// ---------- 追加来源(多数据源/多库, 每张源表可指向不同数据源/库) ----------
const extraGroups = ref([])
function addExtra() { extraGroups.value.push({ ds: '', db: '', dbs: [], tables: [], selected: [] }) }
function removeExtra(i) { extraGroups.value.splice(i, 1) }
async function onEgDsChange(i) {
  const g = extraGroups.value[i]
  g.db = ''; g.dbs = []; g.tables = []; g.selected = []
  if (!g.ds) return
  g.dbs = (await datasourceDatabases(g.ds)) || []
}
async function onEgDbChange(i) {
  const g = extraGroups.value[i]
  g.tables = []; g.selected = []
  if (!g.ds || !g.db) return
  const schs = (await datasourceSchemas(g.ds, g.db)) || []
  const sch = schs.length ? schs[0] : null
  g.tables = (await datasourceDbTables(g.ds, g.db, sch)) || []
}
const extraDsOf = id => datasources.value.find(d => d.id === id)?.name || '数据源' + id
const allTables = computed(() => {
  const set = [...form.value.tables]
  extraGroups.value.forEach(g => (g.selected || []).forEach(t => { if (!set.includes(t)) set.push(t) }))
  return set
})
const tableDup = computed(() => {
  const m = {}; const dup = []
  const push = n => { m[n] = (m[n] || 0) + 1; if (m[n] > 1 && !dup.includes(n)) dup.push(n) }
  form.value.tables.forEach(push)
  extraGroups.value.forEach(g => (g.selected || []).forEach(push))
  return dup
})
const anyExtraSel = computed(() => extraGroups.value.some(g => (g.selected || []).length > 0))
const dsSummary = computed(() => {
  const parts = []
  if (form.value.ds) parts.push(dsName.value)
  extraGroups.value.filter(g => g.ds && g.selected.length).forEach(g => parts.push(extraDsOf(g.ds)))
  return parts.join(' + ') || '—'
})
function originOf(t) {
  for (const g of extraGroups.value) {
    if ((g.selected || []).includes(t)) {
      return { ds: g.ds, db: g.db || g.dbs[0] || '' }
    }
  }
  return { ds: form.value.ds, db: form.value.db || form.value.schema }
}
const dsNameOf = t => {
  const o = originOf(t)
  const d = datasources.value.find(x => x.id === o.ds)
  return d ? d.name : dsName.value
}
onMounted(async () => {
  loadDatasources()
  try {
    const ms = await listModels()
    publishedModels.value = (ms || []).map(m => ({
      id: String(m.id), name: m.name, layer: m.layer, tableName: m.table_name,
      fields: (() => { try { const f = JSON.parse(m.fields || '[]'); return f.map(x => x.name) } catch { return [] } })()
    }))
  } catch { /* 模型接口不可用时用 mock 兜底 */ }
  // 模型页"配同步任务"跳转: 预选模型
  if (route.query.model) {
    form.value.model = String(route.query.model)
    onModelChange()
  }
})

const mockModels = [
  { id: 'm-dwd-order', name: '订单明细', layer: 'DWD', tableName: 't_order', fields: ['order_id', 'order_no', 'customer_id', 'customer_mobile', 'amount', 'amount_tax', 'status', 'order_ts', 'seq'] },
  { id: 'm-dwd-cust', name: '客户主数据', layer: 'DWD', tableName: 't_customer', fields: ['customer_id', 'customer_name', 'mobile', 'level', 'created_ts', 'seq'] },
  { id: 'm-dws-order', name: '订单汇总', layer: 'DWS', tableName: 't_order_dws', fields: ['order_date', 'region', 'order_count', 'amount_sum', 'seq'] },
  { id: 'm-dwd-cust-wide', name: '客户宽表(多表合并)', layer: 'DWD', tableName: 't_user_wide', fields: ['id_card_no', 'name', 'phone', 'addr', 'level', 'seq'] }
]
const publishedModels = ref([])
const fieldTypes = {
  order_id: 'BIGINT', order_no: 'VARCHAR(32)', customer_id: 'BIGINT', customer_mobile: 'VARCHAR(16)', amount: 'DECIMAL(18,2)',
  amount_tax: 'DECIMAL(18,2)', status: 'INT', order_ts: 'TIMESTAMP', seq: 'BIGINT',
  customer_name: 'VARCHAR(64)', mobile: 'VARCHAR(16)', level: 'INT', created_ts: 'TIMESTAMP',
  order_date: 'DATE', region: 'VARCHAR(32)', order_count: 'BIGINT', amount_sum: 'DECIMAL(18,2)',
  id_card_no: 'VARCHAR(32)', name: 'VARCHAR(64)', phone: 'VARCHAR(16)', addr: 'VARCHAR(128)'
}
// 实例 → 库 → Schema → 表 的级联选择(真实 API)
const dbType = computed(() => datasources.value.find(d => d.id === form.value.ds)?.type || 'mysql')
const dbLabel = computed(() => dbLevels(dbType.value).dbLabel)
const tableLabel = computed(() => dbLevels(dbType.value).tableLabel)
// Oracle 无 database 层: databases 接口返回的就是 Schema(用户)列表
const catalogOptions = computed(() => dbType.value === 'oracle' ? [] : dsDatabases.value)
const schemaOptions = computed(() => dbType.value === 'oracle' ? dsDatabases.value : dsSchemas.value)
const dsTables = computed(() => dsTableObjs.value)

async function onDsChange() {
  form.value.tables = []; form.value.db = ''; form.value.schema = ''; curTable.value = ''
  columnsCache.value = {}; dsDatabases.value = []; dsSchemas.value = []; dsTableObjs.value = []
  if (!form.value.ds) return
  dsDatabases.value = (await datasourceDatabases(form.value.ds)) || []
}
async function onDbChange() {
  form.value.tables = []; form.value.schema = ''; curTable.value = ''
  dsSchemas.value = []; dsTableObjs.value = []
  if (!form.value.ds || !form.value.db) return
  dsSchemas.value = (await datasourceSchemas(form.value.ds, form.value.db)) || []
  if (!dsSchemas.value.length) {
    dsTableObjs.value = await loadTables()
  }
}
async function onSchemaChange() {
  form.value.tables = []; curTable.value = ''
  dsTableObjs.value = await loadTables()
}
async function loadTables() {
  if (!form.value.ds) return []
  const db = form.value.db || form.value.schema   // oracle: schema 即 database 参数
  const sch = dbType.value === 'oracle' ? null : form.value.schema
  return (await datasourceDbTables(form.value.ds, db, sch)) || []
}
// 选中表后按需拉字段(主键候选)
watch(() => [...allTables.value], async (tables) => {
  for (const t of tables) {
    if (columnsCache.value[t]) continue
    const o = originOf(t)
    if (!o.ds) { columnsCache.value[t] = []; continue }
    try {
      columnsCache.value[t] = (await datasourceDbColumns(o.ds, o.db, t, null)) || []
    } catch { columnsCache.value[t] = [] }
  }
})
const tableConfigs = computed(() => allTables.value.map(name => {
  const meta = dsTableObjs.value.find(t => t.name === name)
  const cols = columnsCache.value[name] || []
  const pk = cols.find(c => c.primaryKey)?.name
  return {
    table: name, mode: '全量+CDC',
    pk: pk || cols[0]?.name || '',
    pkCandidates: cols.map(c => c.name),
    system: dsNameOf(name),
    rows: meta?.type === 'VIEW' ? '视图' : '—'
  }
}))

// ---------- 模型字段映射(来源 + 加工链) ----------
const sourceKinds = [
  { id: 'field-map', name: '字段映射' },
  { id: 'compute', name: '计算字段(表达式)' },
  { id: 'uuid', name: 'UUID 生成' },
  { id: 'system', name: '系统生成' }
]
const stepKinds = [
  { id: 'null-fill', name: '空值填充', desc: '空值替换为默认值' },
  { id: 'split', name: '字段拆分', desc: '按分隔符拆成多字段' },
  { id: 'case', name: '大小写转换', desc: 'upper / lower' },
  { id: 'truncate', name: '字段截断', desc: '限制最大长度' },
  { id: 'mask', name: '字段脱敏', desc: '手机号/身份证/金额' },
  { id: 'json-parse', name: 'JSON 解析', desc: '从 JSON 提取字段' },
  { id: 'date', name: '日期处理', desc: '格式/时区转换' },
  { id: 'cast', name: '类型转换', desc: '字段类型转换' },
  { id: 'trim', name: '去空格', desc: '去除首尾空白' },
  { id: 'replace', name: '替换', desc: '值替换/正则' },
  { id: 'enum', name: '枚举映射', desc: '字典值标准化' }
]
const stepName = id => stepKinds.find(k => k.id === id)?.name || id
const fieldMapping = ref([])
const demoMapping = {
  'm-dwd-order': [
    { modelField: 'order_id', source: { kind: 'field-map', srcTable: 't_order', srcField: 'order_id' }, steps: [] },
    { modelField: 'order_no', source: { kind: 'field-map', srcTable: 't_order', srcField: 'order_no' }, steps: [{ kind: 'trim', params: {} }] },
    { modelField: 'customer_id', source: { kind: 'field-map', srcTable: 't_order', srcField: 'customer_id' }, steps: [] },
    { modelField: 'customer_mobile', source: { kind: 'field-map', srcTable: 't_order', srcField: 'mobile' }, steps: [
      { kind: 'null-fill', params: { fillValue: '未知' } }, { kind: 'mask', params: { maskType: 'phone' } }
    ]},
    { modelField: 'amount', source: { kind: 'compute', srcTable: 't_order', expr: 'amount / 100' }, steps: [{ kind: 'cast', params: { targetType: 'DECIMAL(18,2)' } }] },
    { modelField: 'amount_tax', source: { kind: 'compute', srcTable: 't_order', expr: 'amount * (1 + tax_rate)' }, steps: [] },
    { modelField: 'order_ts', source: { kind: 'field-map', srcTable: 't_order', srcField: 'create_time' }, steps: [{ kind: 'date', params: { format: '%Y-%m-%d %H:%i:%s' } }] },
    { modelField: 'seq', source: { kind: 'system', srcTable: '系统生成' }, steps: [] }
  ],
  'm-dwd-cust': [
    { modelField: 'customer_id', source: { kind: 'field-map', srcTable: 't_customer', srcField: 'customer_id' }, steps: [] },
    { modelField: 'customer_name', source: { kind: 'field-map', srcTable: 't_customer', srcField: 'name' }, steps: [{ kind: 'trim', params: {} }] },
    { modelField: 'mobile', source: { kind: 'field-map', srcTable: 't_customer', srcField: 'mobile' }, steps: [{ kind: 'mask', params: { maskType: 'phone' } }] },
    { modelField: 'level', source: { kind: 'field-map', srcTable: 't_customer', srcField: 'level' }, steps: [{ kind: 'enum', params: { dict: '1/2/3→普通/银/金' } }] },
    { modelField: 'created_ts', source: { kind: '', srcTable: 't_customer' }, steps: [] },
    { modelField: 'seq', source: { kind: 'system', srcTable: '系统生成' }, steps: [] }
  ],
  // 多表合并演示: t_user_base(身份/联系方式) + t_user_ext(住址/等级) → 客户宽表, 公共业务键 id_card_no
  'm-dwd-cust-wide': [
    { modelField: 'id_card_no', source: { kind: 'field-map', srcTable: 't_user_base', srcField: 'id_card_no' }, steps: [] },
    { modelField: 'name', source: { kind: 'field-map', srcTable: 't_user_base', srcField: 'name' }, steps: [] },
    { modelField: 'phone', source: { kind: 'field-map', srcTable: 't_user_base', srcField: 'phone' }, steps: [] },
    { modelField: 'addr', source: { kind: 'field-map', srcTable: 't_user_ext', srcField: 'addr' }, steps: [] },
    { modelField: 'level', source: { kind: 'field-map', srcTable: 't_user_ext', srcField: 'level' }, steps: [] },
    { modelField: 'seq', source: { kind: 'system', srcTable: '系统生成' }, steps: [] }
  ]
}
// ---------- 业务主键 ----------
// 系统名候选: 真实数据源名称(可再新增, 组件支持 allow-create)
const systemOptions = computed(() => [...new Set(datasources.value.map(d => d.name))])
const modelFields = computed(() => {
  const m = publishedModels.value.find(x => x.id === form.value.model)
  return m ? m.fields.map(f => ({ name: f, type: fieldTypes[f] || 'VARCHAR(64)' })) : []
})
// 勾选系统名称时,若未填取值,自动预填当前数据源的名称(可再增删)
watch(() => form.value.businessKey.scopeFields, (fields, prev) => {
  if (fields.includes('system_name') && !(prev || []).includes('system_name') && !form.value.businessKey.systemNames.length) {
    const ds = datasources.value.find(d => d.id === form.value.ds)
    if (ds?.name) form.value.businessKey.systemNames = [ds.name]
  }
})
// 校验规则与 BusinessKeyConfig 组件保持一致
const bkValid = computed(() => {
  const bk = form.value.businessKey
  if (!bk.keyFields.length) return false
  if (bk.mode === 'scope' && !bk.scopeFields.length) return false
  if (bk.mode === 'scope' && bk.scopeFields.includes('system_name') && !(bk.systemNames || []).length) return false
  if (bk.scopeFields.includes('datasource_id') || bk.scopeFields.includes('database_name') || bk.scopeFields.includes('schema_name')) return bk.confirmPhysical
  return true
})
const bkSummary = computed(() => {
  const bk = form.value.businessKey
  if (!bk.keyFields.length) return '未配置'
  const mode = bk.mode === 'scope' ? '区分' : '合并'
  const parts = [...(bk.mode === 'scope' ? bk.scopeFields : []), ...bk.keyFields]
  const pretty = parts.map(p => p === 'system_name' ? `system_name[${(bk.systemNames || []).join('|') || '?'}]` : p)
  return `${mode}: ${pretty.join(' + ')}`
})
// 智能推荐:源表主键 ∩ 模型字段 → 预填业务键
function suggestKeyFields() {
  const bk = form.value.businessKey
  if (bk.keyFields.length) return
  const modelSet = new Set((publishedModels.value.find(m => m.id === form.value.model)?.fields || []))
  const pks = new Set(Object.values(columnsCache.value).flat().filter(c => c.primaryKey).map(c => c.name))
  const hit = [...pks].find(p => modelSet.has(p))
  if (hit) bk.keyFields = [hit]
}
function onModelChange() {
  const m = publishedModels.value.find(x => x.id === form.value.model)
  if (!m) { fieldMapping.value = []; return }
  suggestKeyFields()
  const demo = demoMapping[form.value.model]
  fieldMapping.value = m.fields.map(f => {
    const d = (demo || []).find(x => x.modelField === f)
    return d
      ? { modelField: f, type: fieldTypes[f] || 'VARCHAR(64)', source: { ...d.source }, steps: d.steps.map(s => ({ kind: s.kind, params: { ...s.params } })) }
      : { modelField: f, type: fieldTypes[f] || 'VARCHAR(64)', source: { kind: '', srcTable: form.value.tables[0] || '', srcField: '', expr: '', related: '', joinKeys: '', resultField: '' }, steps: [] }
  })
}
function onSourceChange(row) {
  if (row.source.kind === 'system') row.source.srcTable = '系统生成'
}
watch(() => form.value.model, () => { if (form.value.model) onModelChange() })

// ---------- 样例数据 + 链式求值 ----------
const sampleData = {
  't_order': { order_id: 100231, order_no: 'SO20260829001', customer_id: 8821, mobile: '13800132210', amount: 2399, tax_rate: 0.13, create_time: '2026-08-29 13:20:11', status: 1, freight: 10, pay_status: 1, is_deleted: 0 },
  't_payment': { pay_id: 100231, pay_no: 'PAY001', pay_amount: 239900, pay_status: 1, pay_time: '2026-08-29 13:22:47' },
  't_customer': { customer_id: 8821, name: '张三', mobile: '13900118822', level: 2 },
  't_user_base': { id_card_no: '110101199001011234', name: '张三', phone: '13800138000' },
  't_user_ext': { id_card_no: '110101199001011234', addr: '北京市朝阳区建国路 88 号', level: 2 }
}
const fnNames = new Set(['concat', 'substr', 'IF', 'round', 'date_format', 'coalesce', 'cast', 'abs', 'floor', 'ceil', 'now', 'length', 'trim', 'replace', 'split', 'sum', 'avg', 'count', 'max', 'min', 'lower', 'upper'])
function evalExpr(expr, ctx) {
  let e = String(expr)
  const names = Object.keys(ctx).sort((a, b) => b.length - a.length)
  for (const n of names) e = e.split(n).join(JSON.stringify(ctx[n]))
  const scope = {
    concat: (...a) => a.join(''),
    substr: (s, st, l) => String(s).substr(st - 1, l),
    round: (x, n) => Number(x).toFixed(n ?? 0),
    IF: (c, a, b) => (c ? a : b),
    coalesce: (...a) => a.find(x => x !== null && x !== undefined && x !== ''),
    date_format: (ts) => String(ts)
  }
  try {
    const fn = new Function(...Object.keys(scope), `return (${e})`)
    return fn(...Object.values(scope))
  } catch { return '—' }
}
function srcFieldList(t) {
  if (t === '系统生成') return []
  // 优先真实列(级联已加载), 缺省回退 mock 示例
  const real = (columnsCache.value[t] || []).map(c => c.name)
  return real.length ? real : Object.keys(sampleData[t] || {})
}
const allRefFields = computed(() => {
  const set = new Set()
  allTables.value.forEach(t => srcFieldList(t).forEach(f => set.add(f)))
  fieldMapping.value.forEach(r => { if (r.source.kind) set.add(r.modelField) })
  return [...set]
})
function maskValue(v, type) {
  const s = String(v)
  if (type === 'phone' && s.length >= 11) return s.slice(0, 3) + '****' + s.slice(7)
  if (type === 'idcard' && s.length >= 14) return s.slice(0, 4) + '**********' + s.slice(-4)
  if (type === 'amount') return s
  return s.slice(0, 2) + '***'
}
function applySteps(v, steps) {
  let val = v
  for (const s of steps) {
    const p = s.params || {}
    switch (s.kind) {
      case 'null-fill': if (val === null || val === undefined || val === '') val = p.fillValue ?? ''; break
      case 'case': val = String(val)[p.caseType === 'lower' ? 'toLowerCase' : 'toUpperCase'](); break
      case 'truncate': val = String(val).slice(0, Number(p.length) || 20); break
      case 'mask': val = maskValue(val, p.maskType); break
      case 'replace': val = String(val).split(p.find || '').join(p.to || ''); break
      case 'trim': val = String(val).trim(); break
      case 'json-parse': try { const o = JSON.parse(val); val = p.path ? o[p.path.replace('$.', '')] : o } catch { val = '—' } break
      case 'date': val = String(val); break
      case 'cast': val = val; break
      case 'enum': val = String(val); break
      default: break
    }
  }
  return val
}
// 预览(按字段定义顺序链式求值,支持字段间依赖)
const previews = computed(() => {
  const ctx = {}
  allTables.value.forEach(t => Object.assign(ctx, sampleData[t] || {}))
  const out = {}
  fieldMapping.value.forEach(row => {
    const s = row.source
    if (!s.kind) { out[row.modelField] = '未定义'; return }
    let v
    if (s.kind === 'field-map') v = ctx[s.srcField] ?? '—'
    else if (s.kind === 'compute') v = s.expr ? evalExpr(s.expr, ctx) : '—'
    else if (s.kind === 'uuid') v = '3f8a-9c2e-…(UUID)'
    else if (s.kind === 'system') v = '(系统生成)'
    const final = applySteps(v, row.steps)
    out[row.modelField] = typeof final === 'string' ? final : JSON.stringify(final)
    ctx[row.modelField] = final
  })
  return out
})

// ---------- 校验 ----------
const undefinedFields = computed(() => fieldMapping.value.filter(r => !r.source.kind).map(r => r.modelField))
const badRefs = computed(() => {
  const out = []
  const seen = new Set(form.value.tables.flatMap(t => srcFieldList(t)))
  fieldMapping.value.forEach(r => {
    const s = r.source
    if (s.kind === 'compute' && s.expr) {
      const refs = s.expr.match(/[a-zA-Z_][a-zA-Z0-9_]*/g) || []
      const unknown = [...new Set(refs.filter(x => !seen.has(x) && !fnNames.has(x)))]
      if (unknown.length) out.push({ modelField: r.modelField, unknown })
    }
    if (s.kind) seen.add(r.modelField)
  })
  return out
})
const typeWarnings = computed(() => fieldMapping.value.filter(r => {
  const s = r.source
  if (s.kind !== 'compute') return false
  const v = previews.value[r.modelField]
  if (!v || v === '—' || v === '未定义' || v === '(系统生成)') return false
  const expectNum = /BIGINT|INT|DECIMAL|DOUBLE|FLOAT/.test(r.type)
  if (expectNum) return !(typeof v === 'number' || (typeof v === 'string' && v !== '' && !isNaN(Number(v))))
  return false
}).map(r => r.modelField))
const hasIssue = computed(() => undefinedFields.value.length > 0 || badRefs.value.length > 0 || typeWarnings.value.length > 0)
const definedCount = computed(() => fieldMapping.value.length - undefinedFields.value.length)
const totalSteps = computed(() => fieldMapping.value.reduce((n, r) => n + r.steps.length, 0))
function isBad(row) {
  return undefinedFields.value.includes(row.modelField) || badRefs.value.some(b => b.modelField === row.modelField) || typeWarnings.value.includes(row.modelField)
}
function rowClass({ row }) { return isBad(row) ? 'conflict-row' : '' }
function sourceSummary(row) {
  const s = row.source
  if (!s.kind) return '未定义'
  if (s.kind === 'field-map') return `映射 ${s.srcTable}.${s.srcField}`
  if (s.kind === 'compute') return `计算 ${s.expr}`
  if (s.kind === 'uuid') return 'UUID'
  return '系统生成'
}

// ---------- 加工步骤编辑 ----------
const stepDialog = ref(false)
const editingStep = ref(null)
const stepForm = ref({ kind: '', params: {} })
function openStep(row) { editingStep.value = null; stepForm.value = { kind: '', params: {} }; stepDialog.value = true; curRow.value = row }
const curRow = ref(null)
function editStep(row, s) { editingStep.value = s; stepForm.value = { kind: s.kind, params: { ...s.params } }; curRow.value = row; stepDialog.value = true }
function onStepKindChange() {
  stepForm.value.params = {}
  if (stepForm.value.kind === 'case') stepForm.value.params.caseType = 'upper'
  if (stepForm.value.kind === 'truncate') stepForm.value.params.length = 20
  if (stepForm.value.kind === 'mask') stepForm.value.params.maskType = 'phone'
}
function confirmStep() {
  if (editingStep.value) { editingStep.value.kind = stepForm.value.kind; editingStep.value.params = { ...stepForm.value.params } }
  else curRow.value.steps.push({ kind: stepForm.value.kind, params: { ...stepForm.value.params } })
  stepDialog.value = false
  ElMessage.success(`加工步骤「${stepName(stepForm.value.kind)}」已${editingStep.value ? '保存' : '添加'}`)
}
function moveStep(row, s, dir) {
  const i = row.steps.indexOf(s); const j = i + dir
  if (j < 0 || j >= row.steps.length) return
  ;[row.steps[i], row.steps[j]] = [row.steps[j], row.steps[i]]
}
function removeStep(row, s) { row.steps.splice(row.steps.indexOf(s), 1) }
function stepSummary(s) {
  const p = s.params || {}
  switch (s.kind) {
    case 'null-fill': return `空值→${p.fillValue ?? ''}`
    case 'case': return p.caseType === 'lower' ? '转小写' : '转大写'
    case 'truncate': return `截断 ${p.length ?? 20} 位`
    case 'mask': return `脱敏(${p.maskType || ''})`
    case 'json-parse': return `取 ${p.path || ''}`
    case 'date': return `格式 ${p.format || ''}`
    case 'split': return `按「${p.sep || ''}」拆 → ${p.newFields || ''}`
    case 'cast': return `转 ${p.targetType || ''}`
    case 'replace': return `${p.find || ''}→${p.to || ''}`
    case 'enum': return p.dict || ''
    default: return ''
  }
}
function stepTypeColor(k) { return { 'null-fill': 'info', split: 'warning', case: 'info', truncate: 'info', mask: 'danger', 'json-parse': 'warning', date: 'primary', cast: 'primary', trim: 'info', replace: 'info', enum: 'success' }[k] || 'info' }

// ---------- 行级转换链 ----------
const chainStore = {
  't_order': [
    { id: 1, type: 'filter', name: '过滤逻辑删除', enabled: true, params: { expr: "is_deleted != 1 && amount >= 0", mode: 'keep' } }
  ],
  't_payment': [
    { id: 1, type: 'filter', name: '过滤退款失败', enabled: true, params: { expr: 'pay_status != 9', mode: 'keep' } }
  ],
  't_customer': []
}
function chainOf(t) { return chainStore[t] || [] }
const transformTypes = [
  { group: '行级过滤(采集端执行)', types: [
    { id: 'filter', name: '过滤 Filter', desc: '按条件保留/丢弃行' },
    { id: 'filter-kind', name: '按操作类型过滤 FilterRowKind', desc: '只保留 INSERT/UPDATE/DELETE 中的指定类型' }
  ]}
]
const nodeDialog = ref(false)
const editingNode = ref(null)
const nodeForm = ref({ type: '', name: '', enabled: true, params: {} })
const typeNames = Object.fromEntries(transformTypes.flatMap(g => g.types).map(t => [t.id, t.name]))
function openNode() { editingNode.value = null; nodeForm.value = { type: '', name: '', enabled: true, params: {} }; nodeDialog.value = true }
function editNode(n) { editingNode.value = n; nodeForm.value = JSON.parse(JSON.stringify(n)); nodeDialog.value = true }
function onTypeChange() {
  nodeForm.value.params = {}
  nodeForm.value.name = typeNames[nodeForm.value.type] || ''
  if (nodeForm.value.type === 'filter') nodeForm.value.params.mode = 'keep'
  if (nodeForm.value.type === 'filter-kind') nodeForm.value.params.kinds = ['INSERT', 'UPDATE']
}
function confirmNode() {
  const n = { ...nodeForm.value, params: { ...nodeForm.value.params } }
  if (editingNode.value) Object.assign(editingNode.value, n)
  else { n.id = Date.now(); chainOf(curTable.value).push(n) }
  nodeDialog.value = false
  ElMessage.success(`行级节点「${n.name}」已${editingNode.value ? '保存' : '添加'}`)
}
function toggleNode(n) { n.enabled = !n.enabled }
function moveNode(n, dir) {
  const arr = chainOf(curTable.value); const i = arr.indexOf(n); const j = i + dir
  if (j < 0 || j >= arr.length) return
  ;[arr[i], arr[j]] = [arr[j], arr[i]]
}
function removeNode(n) { chainOf(curTable.value).splice(chainOf(curTable.value).indexOf(n), 1) }
function makeSummary(n) {
  const p = n.params || {}
  switch (n.type) {
    case 'filter': return `${p.mode === 'drop' ? '丢弃' : '保留'} ${p.expr || ''}`
    case 'filter-kind': return `保留 ${(p.kinds || []).join('/') || '—'}`
    default: return ''
  }
}
function nodeTypeColor(t) { return { filter: 'danger', 'filter-kind': 'danger' }[t] || 'info' }

// ---------- 派生 ----------
const modelName = computed(() => publishedModels.value.find(m => m.id === form.value.model)?.name || '—')
const modelLayer = computed(() => publishedModels.value.find(m => m.id === form.value.model)?.layer || '—')
const dsName = computed(() => datasources.value.find(d => d.id === form.value.ds)?.name || '—')
const totalChain = computed(() => allTables.value.reduce((n, t) => n + chainOf(t).filter(x => x.enabled).length, 0))
const canNext = computed(() => {
  if (step.value === 0) return form.value.ds && form.value.model && form.value.tables.length > 0 && !tableDup.value.length
  if (step.value === 1) return bkValid.value
  return true
})

function onStartTime(val) {
  if (val) {
    form.value.startValue = String(new Date(val.replace(' ', 'T')).getTime())
  }
}

async function submit() {
  if (hasIssue.value) { ElMessage.warning('存在未定义或校验问题,请先处理'); return }
  if (!bkValid.value) { ElMessage.warning('业务主键未配置或校验不通过,请先处理'); return }
  const m = publishedModels.value.find(x => x.id === form.value.model)
  const tableName = m?.tableName || allTables.value[0] || 't_order'
  try {
    // 业务主键配置 → 元库 logical_table(键构成 = 路由/收敛依据)
    await saveLogicalTable(tableName, {
      topic: `dcp.${tableName}`,
      tablePrefix: tableName,
      tableCount: m?.tableCount || 50,
      partitionCount: m?.partitionCount || 8,
      consumerGroup: `dcp-sync-${tableName}`,
      keyColumns: form.value.businessKey.keyFields,
      businessKey: {
        mode: form.value.businessKey.mode,
        scopeFields: form.value.businessKey.scopeFields,
        systemNames: form.value.businessKey.systemNames || [],
        keyFields: form.value.businessKey.keyFields
      },
      syncMode: form.value.syncMode,
      fullScope: form.value.syncMode === 'full-once' ? form.value.fullScope : null,
      cronExpr: (form.value.syncMode === 'scheduled-batch' || form.value.syncMode === 'full-scheduled') ? form.value.cronExpr || null : null,
      startMode: (form.value.syncMode === 'incremental' || form.value.syncMode === 'scheduled-batch') ? form.value.startMode : null,
      startValue: (form.value.syncMode === 'incremental' || form.value.syncMode === 'scheduled-batch') ? form.value.startValue : null
    })
    // 字段映射 + 行级链 + 业务主键 → 同步任务配置(实现 1: 后端校验并保存; 多源表 = 自动注册采集任务)
    const savedRes = await saveSyncTask({
      name: `${modelName.value}-${tableName}`,
      logicalTable: tableName,
      datasourceId: form.value.ds,
      sourceDb: form.value.db || form.value.schema,
      sourceTable: form.value.tables[0],
      sourceTables: form.value.tables,
      fieldMapping: fieldMapping.value,
      rowChain: Object.fromEntries(allTables.value.map(t => [t, chainOf(t)])),
      sourceTables: allTables.value,
      asNewInstance: asNew.value,
      sources: [
        { datasourceId: form.value.ds, sourceDb: form.value.db || form.value.schema, sourceTables: form.value.tables },
        ...extraGroups.value.filter(g => g.ds && (g.selected || []).length)
          .map(g => ({ datasourceId: g.ds, sourceDb: g.db || g.dbs[0] || '', sourceTables: g.selected }))
      ],
      businessKey: form.value.businessKey,
      syncMode: form.value.syncMode,
      fullScope: form.value.fullScope,
      cronExpr: form.value.cronExpr || null,
      startMode: form.value.startMode
    })
    // 后端预校验(字段映射/字段流唯一性等)以 {saved:false, issues:[...]} 返回; 旧版前端会把它当成功吞掉
    if (savedRes?.saved === false) {
      throw new Error((savedRes.issues || []).join('; ') || '保存被拒绝(校验未通过)')
    }
    const mergeTip = allTables.value.length > 1 ? `(多表合并: ${allTables.value.join(' + ')} → 宽表 ${tableName})` : ''
    ElMessage.success(`已提交审批(业务主键: ${bkSummary.value})→ 任务配置已保存, ${mergeTip} 在任务列表启动采集后自动全量+CDC 收敛`)
    setTimeout(() => { router.push('/sync/tasks') }, 1200)
  } catch (e) {
    ElMessage.error('逻辑表配置保存失败: ' + (e?.message || e))
  }
}
</script>

<style scoped>
.eg-card { border: 1px solid #e8e8e8; border-radius: 10px; padding: 10px 14px; margin-top: 10px; background: #fafcff; }
.mode-cards { display: flex; flex-wrap: nowrap; gap: 8px; width: 100%; }
.mode-cards .mode-card { margin: 0; height: auto; white-space: normal; padding: 8px 10px; flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; line-height: 1.4; }
.mode-cards .mode-card .sub { font-size: 11px; color: #8f959e; }
.sec-title { font-size: 14px; font-weight: 600; margin-bottom: 12px; }
.mt16 { margin-top: 16px; }
.flex-between { display: flex; justify-content: space-between; }
.tbl-item { padding: 10px 12px; border: 1px solid #f0f0f0; border-radius: 8px; margin-bottom: 8px; cursor: pointer; }
.tbl-item:hover { border-color: #1677ff; }
.tbl-item.active { border-color: #1677ff; background: #e6f4ff; }
.tbl-name { font-weight: 600; font-size: 13px; }
.tbl-meta { font-size: 11px; color: #8f959e; margin-top: 2px; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 12px; }
.mono.preview { color: #1677ff; }
.field-chips { display: flex; flex-wrap: wrap; gap: 6px; }
.chip { cursor: pointer; }
.chip:hover { border-color: #1677ff; color: #1677ff; }
.field-chain { background: #fafcff; border: 1px solid #f0f0f0; border-radius: 10px; padding: 14px 16px; }
.fc-section { margin-bottom: 12px; }
.fc-section:last-child { margin-bottom: 0; }
.fc-label { font-size: 12.5px; font-weight: 600; color: #646a73; margin-bottom: 8px; }
.fc-row { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.fc-steps { display: flex; flex-direction: column; gap: 0; }
.fc-step { display: flex; align-items: center; gap: 8px; background: #fff; border: 1px solid #e8e8e8; border-radius: 8px; padding: 6px 10px; }
.fc-summary { color: #646a73; font-size: 12px; flex: 1; min-width: 0; }
.fc-ops { display: flex; gap: 6px; flex-shrink: 0; }
.fc-arrow { text-align: center; color: #c0c4cc; line-height: 1; padding: 2px 0; }
.chain { background: #fafcff; border: 1px solid #f0f0f0; border-radius: 10px; padding: 14px; }
.chain-input, .chain-output { font-size: 12px; font-weight: 600; color: #1677ff; background: #e6f4ff; border-radius: 6px; padding: 6px 10px; display: inline-block; }
.chain-output { color: #389e0d; background: #f6ffed; }
.chain-arrow { text-align: center; color: #c0c4cc; line-height: 1; padding: 4px 0; font-size: 14px; }
.chain-node { display: flex; align-items: center; justify-content: space-between; gap: 10px; background: #fff; border: 1px solid #d9d9d9; border-radius: 8px; padding: 8px 12px; }
.chain-node.disabled { opacity: .55; }
.chain-node .cn-main { display: flex; align-items: center; gap: 8px; min-width: 0; }
.chain-node .cn-name { font-weight: 600; font-size: 13px; white-space: nowrap; }
.chain-node .cn-summary { color: #646a73; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 420px; }
.chain-node .cn-ops { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
.conflict-row { background: #fff1f0 !important; }
.tip { margin-top: 12px; font-size: 12px; color: #8f959e; }
</style>

<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex-between">
        <div>
          <div class="pg-title">数据订阅中心<el-tag size="small" effect="plain" style="margin-left:8px" type="success">已接后端</el-tag></div>
          <el-text size="small" type="info">
            对外数据供给。选数 = <b>数据视图构造器</b>(逻辑表维 ∪ 标签维, 可组合回表); 交付 = <b>PULL 数据服务 API</b> / <b>PUSH Kafka·Webhook(HMAC 验签)</b>; 申请 → 审批 → 生效签发凭据, 全审计。
          </el-text>
        </div>
        <div class="flex" style="gap:8px">
          <el-button size="small" @click="loadAll">刷新</el-button>
          <el-button size="small" type="primary" @click="tab = 'create'">＋ 新建订阅</el-button>
        </div>
      </div>

      <el-tabs v-model="tab" style="margin-top:8px">
        <!-- ① 订阅契约 -->
        <el-tab-pane label="① 订阅契约" name="contracts">
          <el-table :data="contracts" size="small" border v-loading="loading">
            <el-table-column label="契约" min-width="170">
              <template #default="{ row }"><div style="font-weight:600">{{ row.name }}</div><el-text size="small" type="info">#{{ row.id }} · {{ row.subscriber }}</el-text></template>
            </el-table-column>
            <el-table-column label="数据视图" min-width="250">
              <template #default="{ row }">
                <template v-if="row.view_type === 'logical'">
                  <el-tag v-for="t in (row.view?.tables || [])" :key="t" size="small" type="primary" effect="plain" style="margin:1px">{{ t }}</el-tag>
                  <el-tag v-if="(row.view?.tables || []).length > 1" size="small" effect="plain">同构合并</el-tag>
                  <div class="dim2">列 {{ (row.view?.cols || []).join(', ') }}</div>
                </template>
                <template v-else>
                  <el-tag v-for="t in (row.view?.tags || [])" :key="t.path" size="small" type="warning" effect="plain" style="margin:1px">{{ t.path }}</el-tag>
                  <div class="dim2">{{ row.view?.op }} {{ shapeName(row.view?.shape) }}{{ row.view?.shape === 'wide' ? ' → ' + (row.view?.joinTable || '') : '' }}</div>
                </template>
              </template>
            </el-table-column>
            <el-table-column label="交付" width="160">
              <template #default="{ row }">
                <el-tag size="small" :type="row.push === 'push' ? 'danger' : 'primary'" effect="plain">{{ row.push.toUpperCase() }}</el-tag>
                <div class="dim2">{{ row.push === 'push' ? (row.target_type || '') : 'API 拉取' }} · {{ row.freq }}</div>
              </template>
            </el-table-column>
            <el-table-column label="密级/有效期" width="110">
              <template #default="{ row }"><div>≤ {{ secName(row.max_secret) }}</div><div class="dim2">{{ row.ttl || '永久' }}</div></template>
            </el-table-column>
            <el-table-column label="状态" width="100">
              <template #default="{ row }"><el-tag size="small" :type="stTag(row.status)" effect="plain">{{ stName(row.status) }}</el-tag></template>
            </el-table-column>
            <el-table-column label="审批" width="150">
              <template #default="{ row }"><div class="dim2">{{ row.approved_by || '—' }}</div><div class="dim2">{{ (row.approved_at || '').slice(0, 16) }}</div></template>
            </el-table-column>
            <el-table-column label="操作" width="240" fixed="right">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="openDetail(row)">详情</el-button>
                <el-button v-if="row.status === 'pending'" size="small" text type="warning" @click="goApprove(row)">审批</el-button>
                <el-button v-if="row.status === 'active'" size="small" text type="danger" @click="stopIt(row)">停用</el-button>
                <el-button v-if="row.status === 'active' && row.push === 'push'" size="small" text type="success" @click="deliver(row)">投递</el-button>
              </template>
            </el-table-column>
          </el-table>
          <el-text v-if="!contracts.length && !loading" size="small" type="info">暂无契约, 点右上「＋ 新建订阅」申请</el-text>
        </el-tab-pane>

        <!-- ② 新建: 数据视图构造器 -->
        <el-tab-pane label="② 新建订阅" name="create">
          <el-steps :active="step" align-center finish-status="success" style="max-width:900px;margin:0 auto 14px">
            <el-step title="构造数据视图" /><el-step title="交付节奏/方式" /><el-step title="目标与协议" /><el-step title="密级/有效期" /><el-step title="确认提交" />
          </el-steps>

          <!-- 步骤1: 视图构造器 -->
          <div v-if="step === 0" style="max-width:920px;margin:auto">
            <el-form label-width="90px" inline size="small">
              <el-form-item label="契约名称"><el-input v-model="w.name" style="width:200px" placeholder="如: 数仓客户视图日快照" /></el-form-item>
              <el-form-item label="订阅方"><el-input v-model="w.subscriber" style="width:160px" placeholder="接入应用名" /></el-form-item>
            </el-form>
            <el-radio-group v-model="w.viewType" size="small">
              <el-radio-button label="logical">逻辑表维度</el-radio-button>
              <el-radio-button label="tag">标签维度</el-radio-button>
            </el-radio-group>

            <!-- 逻辑表面板 -->
            <div v-if="w.viewType === 'logical'" class="panel">
              <div class="sec-title">① 逻辑表(多选, 同构可合并)</div>
              <div class="chips">
                <el-check-tag v-for="t in catalog.tables" :key="t.name" :checked="w.tables.includes(t.name)" @change="toggleTable(t.name)">
                  {{ t.name }}<span class="dim2"> · {{ t.rows }}行{{ t.incrementKey ? ' · 增量' : '' }}</span>
                </el-check-tag>
              </div>
              <el-radio-group v-if="w.tables.length > 1" v-model="w.mergeMode" size="small" style="margin:6px 0">
                <el-radio label="merge">同构合并(输出行带 source_table)</el-radio>
                <el-radio label="separate">分别契约(拆多条)</el-radio>
              </el-radio-group>
              <el-text v-if="!w.tables.length" size="small" type="warning">请先选择至少一张逻辑表</el-text>

              <template v-if="w.tables.length">
                <div class="sec-title">② 交付字段({{ w.cols.length }})</div>
                <div class="chips">
                  <el-check-tag v-for="c in candCols" :key="c" :checked="w.cols.includes(c)" @change="toggleCol(c)">{{ c }}</el-check-tag>
                </div>
                <el-button v-if="w.cols.length !== candCols.length" size="small" text @click="w.cols = [...candCols]">全选</el-button>

                <div class="sec-title">③ 行条件(组内 AND; 多组间 AND)</div>
                <div v-for="(g, gi) in w.condGroups" :key="gi" class="cond-group">
                  <div v-for="(c, ci) in g.conds" :key="ci" class="cond-line">
                    <el-select v-model="c.field" size="small" style="width:150px"><el-option v-for="f in candCols" :key="f" :label="f" :value="f" /></el-select>
                    <el-select v-model="c.op" size="small" style="width:90px"><el-option v-for="o in ['=', '!=', '>', '>=', '<', '<=', 'in', 'like', 'null']" :key="o" :label="o" :value="o" /></el-select>
                    <el-input v-if="c.op !== 'null'" v-model="c.val" size="small" style="width:150px" placeholder="值 / 逗号列表" />
                    <el-button v-if="g.conds.length > 1" size="small" text type="danger" @click="g.conds.splice(ci, 1)">删</el-button>
                  </div>
                  <div class="flex" style="gap:6px">
                    <el-button size="small" text @click="g.conds.push({ field: candCols[0], op: '=', val: '' })">＋ 条件</el-button>
                    <el-tag v-if="gi > 0" size="small">组间 AND</el-tag>
                  </div>
                </div>
                <el-button size="small" @click="w.condGroups.push({ conds: [{ field: candCols[0], op: '=', val: '' }] })">＋ 条件组</el-button>
              </template>
            </div>

            <!-- 标签面板 -->
            <div v-else class="panel">
              <div class="sec-title">① 标签圈选(可多选)</div>
              <div class="chips">
                <el-check-tag v-for="t in catalog.tags" :key="t.path" :checked="w.tags.some(x => x.path === t.path)" @change="toggleTag(t.path)">
                  {{ t.path }}<span class="dim2"> · {{ t.entities }}实体</span>
                </el-check-tag>
              </div>
              <el-text v-if="!catalog.tags.length" size="small" type="info">暂无打标数据(标签事实为空)</el-text>
              <div class="flex" style="gap:12px;margin:8px 0">
                <el-radio-group v-model="w.tagOp" size="small">
                  <el-radio label="AND">全部命中</el-radio>
                  <el-radio label="OR">任一命中</el-radio>
                </el-radio-group>
                <span class="dim2">排除标签:</span>
                <el-select v-model="w.exclude" multiple clearable size="small" style="width:300px">
                  <el-option v-for="t in catalog.tags" :key="t.path" :label="t.path" :value="t.path" />
                </el-select>
              </div>
              <div class="sec-title">② 输出形态</div>
              <el-radio-group v-model="w.exportShape" size="small">
                <el-radio-button label="entity">仅实体键</el-radio-button>
                <el-radio-button label="matrix">实体 + 标签值矩阵</el-radio-button>
                <el-radio-button label="wide">人群宽表(回表字段)</el-radio-button>
              </el-radio-group>
              <div v-if="w.exportShape === 'wide'" class="flex" style="gap:8px;margin-top:8px;flex-wrap:wrap">
                <span class="dim2">回表:</span>
                <el-select v-model="w.joinTable" size="small" style="width:200px">
                  <el-option v-for="t in catalog.tables" :key="t.name" :label="`${t.name} (${t.rows}行)`" :value="t.name" />
                </el-select>
                <el-select v-model="w.joinKey" size="small" style="width:120px" placeholder="join 键">
                  <el-option v-for="c in joinTableCols" :key="c" :label="c" :value="c" />
                </el-select>
                <span class="dim2">字段:</span>
                <el-check-tag v-for="c in joinTableCols" :key="c" :checked="w.joinCols.includes(c)" @change="toggleJoin(c)">{{ c }}</el-check-tag>
              </div>
              <div class="dim2" style="margin-top:6px">matrix: 每实体一行, 列 = 标签当前值(未命中 ∅); wide: 按实体键 join 回表取字段</div>
            </div>

            <!-- 预览 -->
            <el-card shadow="never" class="sub" style="margin-top:10px">
              <template #header><b>视图预览</b></template>
              <div class="flex" style="gap:6px;flex-wrap:wrap">
                <el-tag v-for="c in previewCols" :key="c" size="small" effect="plain">{{ c }}</el-tag>
              </div>
              <div class="dim2" style="margin-top:6px">{{ previewDesc }}</div>
            </el-card>
            <div style="text-align:center;margin-top:10px">
              <el-button type="primary" :disabled="!canStep1" @click="step = 1">下一步: 交付方式</el-button>
            </div>
          </div>

          <!-- 步骤2: 交付 -->
          <div v-if="step === 1" style="max-width:760px;margin:auto">
            <el-form label-width="110px">
              <el-form-item label="交付方式">
                <el-radio-group v-model="w.push">
                  <el-radio-button label="pull"><b>PULL 拉取</b><div class="dim2">订阅方调数据服务 API 取数(快照/增量游标/导出)</div></el-radio-button>
                  <el-radio-button label="push"><b>PUSH 推送</b><div class="dim2">平台主动推送(Kafka / Webhook)</div></el-radio-button>
                </el-radio-group>
              </el-form-item>
              <el-form-item label="获取节奏">
                <el-radio-group v-model="w.freq">
                  <el-radio-button label="定时">定时</el-radio-button>
                  <el-radio-button label="实时">实时</el-radio-button>
                  <el-radio-button label="批量">批量全量</el-radio-button>
                  <el-radio-button label="流式">流式增量</el-radio-button>
                </el-radio-group>
              </el-form-item>
              <div class="hint">逻辑表增量 = 平台 _seq 游标(changes?after); PULL 快照/增量走 API, PUSH 定时/流式推事件。本期投递为手动触发, 定时调度后续接入。</div>
            </el-form>
            <div style="text-align:center;display:flex;gap:8px;justify-content:center">
              <el-button @click="step = 0">上一步</el-button><el-button type="primary" @click="step = 2">下一步: 目标与协议</el-button>
            </div>
          </div>

          <!-- 步骤3: 目标与协议 -->
          <div v-if="step === 2" style="max-width:860px;margin:auto">
            <template v-if="w.push === 'push'">
              <el-form label-width="110px">
                <el-form-item label="推送目标">
                  <el-radio-group v-model="w.targetType">
                    <el-radio label="kafka">Kafka Topic</el-radio>
                    <el-radio label="webhook">HTTP API(Webhook)</el-radio>
                  </el-radio-group>
                </el-form-item>
                <el-form-item v-if="w.targetType === 'webhook'" label="目标 URL">
                  <el-input v-model="w.webhook" placeholder="https://consumer.example.com/api/data" style="width:420px" />
                </el-form-item>
              </el-form>
              <div class="sec-title">{{ w.targetType === 'kafka' ? 'Kafka 交付协议' : 'Webhook 交付协议' }}</div>
              <pre class="mono">{{ w.targetType === 'kafka' ? kafkaProto : webhookProto }}</pre>
            </template>
            <template v-else>
              <div class="sec-title">PULL 拉取协议(数据服务 API · X-Dcp-Token 鉴权)</div>
              <pre class="mono">{{ pullProto }}</pre>
            </template>
            <div style="text-align:center;display:flex;gap:8px;justify-content:center;margin-top:10px">
              <el-button @click="step = 1">上一步</el-button><el-button type="primary" @click="step = 3">下一步</el-button>
            </div>
          </div>

          <!-- 步骤4: 密级/有效期 -->
          <div v-if="step === 3" style="max-width:760px;margin:auto">
            <el-form label-width="110px">
              <el-form-item label="密级上限">
                <el-select v-model="w.maxSecret" style="width:200px"><el-option v-for="(n, i) in ['公开', '内部', '机密', '绝密']" :key="i + 1" :label="n" :value="i + 1" /></el-select>
              </el-form-item>
              <el-form-item label="有效期"><el-date-picker v-model="w.ttl" type="date" value-format="YYYY-MM-DD" style="width:220px" placeholder="默认永久" /></el-form-item>
              <div class="hint">审批时平台可按管理员授权范围收窄列与密级; 生效后自动签发 X-Dcp-Token(pull)/Webhook 验签密钥(push)。</div>
            </el-form>
            <div style="text-align:center;display:flex;gap:8px;justify-content:center">
              <el-button @click="step = 2">上一步</el-button><el-button type="primary" @click="step = 4">下一步</el-button>
            </div>
          </div>

          <!-- 步骤5 确认 -->
          <div v-if="step === 4" style="max-width:860px;margin:auto">
            <el-descriptions :column="1" size="small" border>
              <el-descriptions-item label="契约">{{ w.name }} · 订阅方 {{ w.subscriber }}</el-descriptions-item>
              <el-descriptions-item label="数据视图">{{ viewSummary }}</el-descriptions-item>
              <el-descriptions-item label="交付">{{ w.push.toUpperCase() }} · {{ w.freq }}<template v-if="w.push === 'push'"> → {{ w.targetType === 'kafka' ? 'Kafka' : w.webhook }}</template></el-descriptions-item>
              <el-descriptions-item label="密级/有效期">≤ {{ secName(w.maxSecret) }} · {{ w.ttl || '永久' }}</el-descriptions-item>
              <el-descriptions-item label="说明">提交后进入审批; 生效后凭据在「详情/审批」中展示; 拉取/投递均审计留痕。</el-descriptions-item>
            </el-descriptions>
            <div style="text-align:center;display:flex;gap:8px;justify-content:center;margin-top:12px">
              <el-button @click="step = 3">上一步</el-button><el-button type="primary" :loading="submitting" @click="submit">提交审批</el-button>
            </div>
          </div>
        </el-tab-pane>

        <!-- ③ 审批 -->
        <el-tab-pane label="③ 审批" name="approve">
          <div v-for="p in pending" :key="p.id" class="ap-card">
            <div class="flex-between"><b>{{ p.name }}</b><el-text size="small" type="info">{{ p.subscriber }} · {{ p.freq }} · {{ p.push.toUpperCase() }}</el-text></div>
            <div class="dim2" style="margin:4px 0">{{ viewDesc(p) }}</div>
            <div class="flex" style="gap:6px">
              <el-button size="small" type="warning" @click="openApprove(p)">审批(可调范围)</el-button>
              <el-button size="small" text type="danger" @click="rejectIt(p)">拒绝</el-button>
            </div>
          </div>
          <el-text v-if="!pending.length" size="small" type="info">暂无待审批</el-text>
        </el-tab-pane>

        <!-- ④ 交付监控 -->
        <el-tab-pane label="④ 交付监控" name="deliver">
          <div class="flex" style="gap:8px;margin-bottom:8px">
            <el-select v-model="monCid" size="small" style="width:320px" placeholder="选择契约" filterable @change="loadDeliveries">
              <el-option v-for="c in contracts" :key="c.id" :label="`#${c.id} ${c.name} (${stName(c.status)})`" :value="c.id" />
            </el-select>
            <el-button v-if="monContract?.status === 'active' && monContract?.push === 'push'" size="small" type="success" @click="deliver(monContract)">立即投递</el-button>
            <el-button v-if="monContract?.status === 'active'" size="small" type="primary" @click="testPull(monContract)">取数测试(前 5 行)</el-button>
            <el-button v-if="monContract?.status === 'active'" size="small" @click="exportCsv(monContract)">导出 CSV</el-button>
            <el-button v-if="monContract?.token" size="small" text type="info" @click="copyTok(monContract.token)">复制 Token</el-button>
          </div>
          <el-table :data="deliveries" size="small" border v-loading="dLoading">
            <el-table-column prop="ts" label="时间" width="170" />
            <el-table-column label="模式/通道" width="160">
              <template #default="{ row }">
                <el-tag size="small" :type="row.mode === 'push' ? 'danger' : 'primary'" effect="plain">{{ row.mode.toUpperCase() }}</el-tag>
                <el-text size="small" class="dim2" style="margin-left:4px">{{ row.channel }}</el-text>
              </template>
            </el-table-column>
            <el-table-column prop="range_text" label="数据范围" width="150" />
            <el-table-column prop="rows_count" label="行数" width="70" align="center" />
            <el-table-column label="结果" width="110">
              <template #default="{ row }">
                <el-tag size="small" :type="row.status === 'ok' ? 'success' : row.status === 'retrying' ? 'warning' : 'danger'" effect="plain">
                  {{ row.status === 'ok' ? '成功' : row.status === 'retrying' ? '重试中' : '失败' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="详情" min-width="220">
              <template #default="{ row }"><div class="dim2">{{ row.error_msg || ('retries=' + row.retries) }}</div></template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <!-- 契约详情(含协议文档/凭据) -->
    <el-drawer v-model="dlg" :title="'契约 #' + (cur ? cur.id : '') + ' · ' + (cur ? cur.name : '')" size="640px">
      <template v-if="cur">
        <el-descriptions :column="1" size="small" border>
          <el-descriptions-item label="订阅方">{{ cur.subscriber }}</el-descriptions-item>
          <el-descriptions-item label="数据视图">{{ viewDesc(cur) }}</el-descriptions-item>
          <el-descriptions-item label="交付">{{ cur.push.toUpperCase() }} · {{ cur.freq }}<template v-if="cur.push === 'push'"> · {{ cur.target_type }}<div v-if="cur.target_url" class="dim2">{{ cur.target_url }}</div></template></el-descriptions-item>
          <el-descriptions-item label="状态">{{ stName(cur.status) }}<template v-if="cur.reject_reason"> · 原因: {{ cur.reject_reason }}</template><div class="dim2">审批 {{ cur.approved_by || '—' }} {{ (cur.approved_at || '').slice(0, 16) }}</div></el-descriptions-item>
          <el-descriptions-item v-if="cur.token" label="X-Dcp-Token(pull)"><code class="tok">{{ cur.token }}</code><el-button size="small" text type="primary" @click="copyTok(cur.token)">复制</el-button></el-descriptions-item>
          <el-descriptions-item v-if="cur.webhook_secret" label="Webhook Secret"><code class="tok">{{ cur.webhook_secret }}</code><el-button size="small" text type="primary" @click="copyTok(cur.webhook_secret)">复制</el-button></el-descriptions-item>
          <el-descriptions-item v-if="cur.push === 'push' && cur.target_type === 'kafka'" label="Topic"><code class="tok">dcp.sub.{{ cur.subscriber }}.{{ cur.id }}</code></el-descriptions-item>
        </el-descriptions>
        <div class="sec-title">交付协议(取数指南)</div>
        <pre class="mono">{{ cur.push === 'pull' ? pullProto : (cur.target_type === 'kafka' ? kafkaProto : webhookProto) }}</pre>
        <div v-if="cur.status === 'active' && cur.push === 'pull'" style="margin-top:8px">
          <el-button size="small" type="primary" @click="testPull(cur)">取数测试(snapshot)</el-button>
          <el-button size="small" @click="exportCsv(cur)">导出 CSV</el-button>
        </div>
        <pre v-if="pullSample" class="mono" style="margin-top:8px">{{ pullSample }}</pre>
      </template>
    </el-drawer>

    <!-- 审批弹层 -->
    <el-dialog v-model="apDlg" :title="'审批 · ' + (apCur ? apCur.name : '')" width="620">
      <template v-if="apCur">
        <el-form label-width="110px" size="small">
          <el-form-item v-if="apCur.view_type === 'logical'" label="交付列(可收窄)">
            <el-checkbox-group v-model="apForm.cols">
              <el-checkbox v-for="c in (apCur.view?.cols || [])" :key="c" :label="c" :value="c" />
            </el-checkbox-group>
          </el-form-item>
          <el-form-item label="密级上限">
            <el-select v-model="apForm.maxSecret" style="width:180px"><el-option v-for="(n, i) in ['公开', '内部', '机密', '绝密']" :key="i + 1" :label="n" :value="i + 1" /></el-select>
          </el-form-item>
          <el-form-item label="备注"><el-input v-model="apForm.note" placeholder="审批意见(写审计)" /></el-form-item>
        </el-form>
        <el-alert v-if="approved" type="success" :closable="false" title="已批准生效 — 凭据仅本次展示, 请立即保存">
          <div>X-Dcp-Token: <code class="tok">{{ approved.token }}</code></div>
          <div v-if="approved.webhookSecret">Webhook Secret: <code class="tok">{{ approved.webhookSecret }}</code></div>
        </el-alert>
      </template>
      <template #footer>
        <el-button size="small" @click="apDlg = false">关闭</el-button>
        <el-button v-if="!approved" size="small" type="success" :loading="apLoading" @click="approve">批准生效</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, watch, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import request from '../../api/request'

const tab = ref('contracts')
const step = ref(0)
const loading = ref(false)
const contracts = ref([])
const dlg = ref(false)
const cur = ref(null)
const apDlg = ref(false)
const apCur = ref(null)
const apForm = ref({ cols: [], maxSecret: 2, note: '' })
const apLoading = ref(false)
const approved = ref(null)
const submitting = ref(false)
const monCid = ref(null)
const deliveries = ref([])
const dLoading = ref(false)
const pullSample = ref('')
const catalog = reactive({ tables: [], tags: [] })

const stMap = { active: ['生效中', 'success'], pending: ['待审批', 'warning'], rejected: ['已拒绝', 'danger'], stopped: ['已停止', 'info'], expired: ['已到期', 'info'] }
const stName = (s) => (stMap[s] || [s, 'info'])[0]
const stTag = (s) => (stMap[s] || ['', 'info'])[1]
const secName = (s) => ['公开', '内部', '机密', '绝密'][(s || 2) - 1] || '内部'
const shapeName = (s) => ({ entity: '实体键', matrix: '标签矩阵', wide: '宽表回表' }[s] || s || '')

/* ---------- 目录 ---------- */
async function loadCatalog() {
  try {
    const d = await request.get('/data-sub/catalog')
    catalog.tables = d?.tables || []
    catalog.tags = d?.tags || []
  } catch (e) { /* 已弹错 */ }
}

/* ---------- 契约列表 ---------- */
async function loadAll() {
  loading.value = true
  try {
    contracts.value = (await request.get('/data-sub')) || []
    if (!monCid.value && contracts.value.length) monCid.value = contracts.value[0].id
  } catch (e) { /* 已弹错 */ } finally { loading.value = false }
}
const pending = computed(() => contracts.value.filter(c => c.status === 'pending'))
const monContract = computed(() => contracts.value.find(c => c.id === monCid.value))

/* ---------- 视图描述 ---------- */
function viewDesc(row) {
  const v = row.view || {}
  if (row.view_type === 'logical') {
    return `${(v.tables || []).join(' + ')}${(v.tables || []).length > 1 ? '(同构合并)' : ''} · 列 ${(v.cols || []).join(',')}${(v.groups || []).some(g => (g.conds || []).length) ? ' · 带行条件' : ''}`
  }
  return `${(v.tags || []).map(t => t.path).join(' ' + (v.op || 'OR') + ' ')}${(v.exclude || []).length ? ' 排除' + v.exclude.length : ''} → ${shapeName(v.shape)}${v.shape === 'wide' ? ' ' + (v.joinTable || '') : ''}`
}
const viewSummary = computed(() => w.value.viewType === 'logical'
  ? (w.value.tables.join(' + ') + (w.value.tables.length > 1 ? '(同构合并)' : '') + ' · 列 ' + w.value.cols.join(','))
  : (w.value.tags.map(t => t.path).join(' ' + w.value.tagOp + ' ') + (w.value.exclude.length ? ' 排除 ' + w.value.exclude.join(',') : '') + ' → ' + shapeName(w.value.exportShape) + (w.value.exportShape === 'wide' ? ' ' + w.value.joinTable + '(' + w.value.joinCols.join(',') + ')' : '')))

/* ---------- 向导 ---------- */
const w = ref({
  name: '', subscriber: '',
  viewType: 'logical',
  tables: [], mergeMode: 'merge', cols: [], condGroups: [{ conds: [{ field: '', op: '>=', val: '5000' }] }],
  tags: [], tagOp: 'OR', exclude: [], exportShape: 'entity', joinTable: '', joinKey: 'id', joinCols: [],
  push: 'pull', freq: '定时', targetType: 'webhook', webhook: '',
  maxSecret: 2, ttl: null
})
const candCols = computed(() => {
  const sel = catalog.tables.filter(t => w.value.tables.includes(t.name))
  if (!sel.length) return []
  const isSys = (c) => c.startsWith('_')
  let sets = sel.map(t => (t.cols || []).filter(c => !isSys(c)).map(c => c))
  if (w.value.tables.length > 1) {
    const inter = sets[0].filter(c => sets.every(s => s.includes(c)))
    return inter
  }
  return sets[0]
})
watch(() => [...w.value.tables], () => {
  if (!w.value.cols.length && candCols.value.length) w.value.cols = [...candCols.value]
})
const joinTableCols = computed(() => (catalog.tables.find(t => t.name === w.value.joinTable)?.cols || []).filter(c => !c.startsWith('_')))
watch(() => w.value.joinTable, () => {
  if (joinTableCols.value.length && !joinTableCols.value.includes(w.value.joinKey)) w.value.joinKey = 'id'
  w.value.joinCols = joinTableCols.value.filter(c => c !== w.value.joinKey && c !== '_is_deleted').slice(0, 6)
})
const previewCols = computed(() => w.value.viewType === 'logical'
  ? w.value.cols
  : ['entity_key', ...(w.value.exportShape === 'wide' ? w.value.joinCols : w.value.exportShape === 'matrix' ? w.value.tags.map(t => t.path) : [])])
const previewDesc = computed(() => {
  if (w.value.viewType === 'logical') {
    const inc = catalog.tables.find(t => t.name === (w.value.tables[0] || ''))?.incrementKey
    return `表 ${w.value.tables.join('+')} · ${w.value.cols.length} 列${inc ? ' · 增量键 ' + inc + '(changes?after 游标)' : ' · 无 _seq 列, 仅快照'}` + (w.value.condGroups.some(g => g.conds.some(c => c.field)) ? ' · 带行条件' : '')
  }
  return `标签圈选 ${w.value.tags.length} 个(${w.value.tagOp}) · 输出 ${shapeName(w.value.exportShape)}`
})
const canStep1 = computed(() => {
  if (!w.value.name || !w.value.subscriber) return false
  if (w.value.viewType === 'logical') return !!w.value.tables.length && !!w.value.cols.length
  return !!w.value.tags.length
})
function toggleTable(t) { const i = w.value.tables.indexOf(t); i >= 0 ? w.value.tables.splice(i, 1) : w.value.tables.push(t) }
function toggleCol(c) { const i = w.value.cols.indexOf(c); i >= 0 ? w.value.cols.splice(i, 1) : w.value.cols.push(c) }
function toggleJoin(c) { const i = w.value.joinCols.indexOf(c); i >= 0 ? w.value.joinCols.splice(i, 1) : w.value.joinCols.push(c) }
function toggleTag(path) {
  const i = w.value.tags.findIndex(t => t.path === path)
  i >= 0 ? w.value.tags.splice(i, 1) : w.value.tags.push({ path })
}
async function submit() {
  submitting.value = true
  try {
    const logical = w.value.viewType === 'logical'
    const view = logical
      ? {
          tables: [...w.value.tables],
          cols: [...w.value.cols],
          merge: w.value.tables.length > 1 ? w.value.mergeMode : 'merge',
          groups: w.value.condGroups.filter(g => (g.conds || []).some(c => c.field && c.op))
            .map(g => ({ conds: g.conds.filter(c => c.field && c.op).map(c => ({ field: c.field, op: c.op, val: c.op === 'null' ? '' : (c.val || '') })) }))
        }
      : {
          tags: w.value.tags,
          op: w.value.tagOp,
          exclude: w.value.exclude.map(path => ({ path })),
          shape: w.value.exportShape,
          joinTable: w.value.exportShape === 'wide' ? w.value.joinTable : null,
          joinKey: w.value.exportShape === 'wide' ? (w.value.joinKey || 'id') : 'entity_key',
          joinCols: w.value.exportShape === 'wide' ? w.value.joinCols : []
        }
    const body = {
      name: w.value.name, subscriber: w.value.subscriber, viewType: w.value.viewType, view,
      push: w.value.push, freq: w.value.freq,
      targetType: w.value.push === 'push' ? w.value.targetType : null,
      targetUrl: w.value.push === 'push' && w.value.targetType === 'webhook' ? w.value.webhook : null,
      maxSecret: w.value.maxSecret, ttl: w.value.ttl || null
    }
    const r = await request.post('/data-sub', body)
    ElMessage.success(`已提交审批 #${r.id}, 待平台批准`)
    step.value = 0
    tab.value = 'approve'
    w.value.name = ''; w.value.subscriber = ''; w.value.tags = []; w.value.tables = []; w.value.cols = []
    await loadAll()
  } catch (e) { /* 已弹错 */ } finally { submitting.value = false }
}

/* ---------- 审批 ---------- */
function goApprove(row) { openApprove(row) }
function openApprove(p) {
  apCur.value = p
  approved.value = null
  apForm.value = { cols: [...((p.view?.cols) || [])], maxSecret: p.max_secret || 2, note: '' }
  apDlg.value = true
}
async function approve() {
  apLoading.value = true
  try {
    const body = {}
    if (apCur.value.view_type === 'logical' && apForm.value.cols.length) body.cols = apForm.value.cols
    body.maxSecret = apForm.value.maxSecret
    const r = await request.post(`/data-sub/${apCur.value.id}/approve`, body)
    approved.value = r
    ElMessage.success('已批准生效; 凭据见下方(仅本次展示)')
    await loadAll()
  } catch (e) { /* 已弹错 */ } finally { apLoading.value = false }
}
async function rejectIt(p) {
  try {
    const { value } = await ElMessageBox.prompt('拒绝原因', `拒绝 · ${p.name}`, { confirmButtonText: '拒绝', cancelButtonText: '取消', inputPlaceholder: '必填' })
    await request.post(`/data-sub/${p.id}/reject`, { reason: value || '未说明' })
    ElMessage.warning('已拒绝')
    await loadAll()
  } catch (e) { if (e !== 'cancel' && e?.message) { /* 已弹错 */ } }
}

/* ---------- 停用/投递/取数 ---------- */
async function stopIt(row) {
  try {
    await ElMessageBox.confirm(`停用契约「${row.name}」后订阅方将无法取数, 继续?`, '停用', { type: 'warning' })
  } catch (e) { return }
  try {
    await request.post(`/data-sub/${row.id}/stop`, {})
    ElMessage.success('已停用')
    await loadAll()
  } catch (e) { /* 已弹错 */ }
}
async function deliver(row) {
  try {
    const r = await request.post(`/data-sub/${row.id}/deliver`, {})
    if (r?.status === 'ok') ElMessage.success(`投递成功: ${r.channel} ${r.rows} 行`)
    else ElMessage.error(`投递失败: ${r?.channel} ${r?.error || ''}`)
    if (monCid.value === row.id) await loadDeliveries()
  } catch (e) { /* 已弹错 */ }
}
async function loadDeliveries() {
  if (!monCid.value) return
  dLoading.value = true
  try {
    deliveries.value = (await request.get(`/data-sub/${monCid.value}/deliveries`)) || []
  } catch (e) { /* 已弹错 */ } finally { dLoading.value = false }
}
async function testPull(row) {
  if (!row.token) { ElMessage.warning('契约未生效, 无取数凭据'); return }
  try {
    const d = await request.get(`/data-sub/${row.id}/snapshot?limit=5`, { headers: { 'X-Dcp-Token': row.token } })
    const rows = d?.rows || []
    pullSample.value = JSON.stringify({ count: d?.count, rows: rows.slice(0, 5), nextCursor: d?.nextCursor }, null, 2)
    ElMessage.success(`拉取 ${d?.count || 0} 行(预览前 5)`)
    if (monCid.value === row.id && tab.value === 'deliver') await loadDeliveries()
  } catch (e) { /* 已弹错 */ }
}
async function exportCsv(row) {
  if (!row.token) { ElMessage.warning('契约未生效, 无取数凭据'); return }
  try {
    const res = await fetch(`/api/data-sub/${row.id}/export?format=csv`, { headers: { 'X-Dcp-Token': row.token } })
    if (!res.ok) throw new Error('HTTP ' + res.status)
    const text = await res.text()
    const blob = new Blob(['\ufeff' + text], { type: 'text/csv;charset=utf-8' })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = `data-sub-${row.id}-${Date.now()}.csv`
    a.click()
    URL.revokeObjectURL(a.href)
    ElMessage.success('CSV 已下载')
    if (monCid.value === row.id && tab.value === 'deliver') await loadDeliveries()
  } catch (e) { ElMessage.error('导出失败: ' + e.message) }
}

/* ---------- 详情 ---------- */
function openDetail(row) {
  pullSample.value = ''
  cur.value = row
  dlg.value = true
}
async function copyTok(t) {
  try { await navigator.clipboard.writeText(t); ElMessage.success('已复制') } catch (e) { ElMessage.error('复制失败') }
}

/* ---------- 协议文档 ---------- */
const pullProto = `# PULL · 数据服务 API
鉴权: Header X-Dcp-Token: {生效签发 token}
GET /api/data-sub/{cid}/schema             # 结构(列/类型/增量键)
GET /api/data-sub/{cid}/snapshot?limit=1000  # 全量快照
GET /api/data-sub/{cid}/changes?after={cursor}&limit=1000  # 增量(_seq 游标)
GET /api/data-sub/{cid}/export?format=csv   # CSV 导出
响应: {"code":0,"data":{"rows":[...],"count":n,"nextCursor":"..."}}
示例: curl -H "X-Dcp-Token: $TOKEN" "http://localhost:8081/api/data-sub/{cid}/changes?after=0"`
const webhookProto = `# PUSH · Webhook 交付(平台 → 订阅方)
POST {目标 URL}
Headers:
  Content-Type: application/json
  X-Dcp-Contract: {cid}
  X-Dcp-Timestamp: 1720000000000
  X-Dcp-Sign: HMAC-SHA256(ts + body, secret=生效下发密钥)
Body: {"contract":"...","count":n,"rows":[{...}]}
订阅方侧: 验签(X-Dcp-Sign) + 防重放(时间戳 ±5min); 2xx=成功, 失败平台退避重试`
const kafkaProto = `# PUSH · Kafka 交付
Topic: dcp.sub.{subscriber}.{contractId}   # 每契约独立(演示直连 kafka:9092, 生产 SASL+ACL)
消息: {"contract":"...","count":n,"rows":[{...}]}
at-least-once; 消费方按主键幂等; 每次投递写交付记录+审计`

onMounted(async () => {
  await loadCatalog()
  await loadAll()
  if (monCid.value) await loadDeliveries()
  // 默认视图给演示表
  if (w.value.viewType === 'logical' && !w.value.tables.length && catalog.tables.length) {
    const demo = catalog.tables.find(t => t.name === 'customer_wide') || catalog.tables[0]
    w.value.tables.push(demo.name)
    w.value.cols = (demo.cols || []).filter(c => !c.startsWith('_'))
  }
  if (catalog.tags.length && w.value.viewType === 'tag' && !w.value.tags.length) {
    w.value.tags.push({ path: catalog.tags[0].path })
  }
})
</script>

<style scoped>
.page-card { max-width: 1580px; }
.pg-title { font-size: 17px; font-weight: 700; }
.flex { display: flex; align-items: center; }
.flex-between { display: flex; justify-content: space-between; align-items: center; }
.dim2 { color: #999; font-size: 12px; }
.chips { display: flex; flex-wrap: wrap; gap: 6px; }
.sec-title { font-size: 13px; font-weight: 600; margin: 10px 0 6px; }
.panel { border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 10px 14px; }
.cond-group { border-left: 2px solid var(--el-color-primary-light-5); padding-left: 10px; margin: 4px 0; }
.cond-line { display: flex; gap: 6px; align-items: center; margin: 4px 0; flex-wrap: wrap; }
.hint { background: #f0f7ff; border: 1px solid #c6e0f7; color: #2a5d8f; padding: 8px 10px; border-radius: 6px; font-size: 12px; }
.mono { background: #0d1b2a; color: #9fdcb0; padding: 10px; font-size: 12px; border-radius: 6px; white-space: pre-wrap; word-break: break-all; }
.ap-card { border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; }
.sub { border: none; }
.tok { font-family: var(--el-font-family-mono); word-break: break-all; color: #b45309; background: #fef3c7; padding: 1px 6px; border-radius: 4px; }
</style>

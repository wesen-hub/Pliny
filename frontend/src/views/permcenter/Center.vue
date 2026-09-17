<template>
  <div class="perm-center">
    <el-tabs v-model="tab" type="border-card" class="pc-tabs" @tab-change="onTabChange">
      <!-- ==================== 总览 ==================== -->
      <el-tab-pane label="策略总览" name="overview">
        <el-row :gutter="12" style="margin-bottom:14px">
          <el-col :span="6" v-for="s in stats" :key="s.label">
            <div class="stat-card">
              <div class="stat-n" :style="{color:s.color||''}">{{ s.n }}</div>
              <div class="stat-l">{{ s.label }}</div>
            </div>
          </el-col>
        </el-row>
        <el-card shadow="never" header="生效点矩阵(策略 → 各出口是否已下发生效)">
          <el-table :data="matrix" size="small" border>
            <el-table-column prop="kind" label="策略类" width="160"/>
            <el-table-column label="平台 REST"><template #default="{row}"><el-tag :type="row['平台 REST']?'success':'info'" size="small">{{ row['平台 REST'] ? '生效' : '不适用' }}</el-tag></template></el-table-column>
            <el-table-column label="v2 计算面"><template #default="{row}"><el-tag :type="row['v2 计算面']?'success':'info'" size="small">{{ row['v2 计算面'] ? '生效' : '阶段二' }}</el-tag></template></el-table-column>
            <el-table-column label="Spark 批"><template #default="{row}"><el-tag :type="row['Spark 批']?'success':'info'" size="small">{{ row['Spark 批'] ? '生效(注入)' : '阶段二' }}</el-tag></template></el-table-column>
            <el-table-column label="Flink 流"><template #default="{row}"><el-tag :type="row['Flink 流']?'success':'info'" size="small">{{ row['Flink 流'] ? '生效(注入)' : '阶段二' }}</el-tag></template></el-table-column>
            <el-table-column label="订阅 PULL"><template #default="{row}"><el-tag :type="row['订阅 PULL']?'success':'info'" size="small">{{ row['订阅 PULL'] ? '生效' : '不适用' }}</el-tag></template></el-table-column>
            <el-table-column label="文件下载"><template #default="{row}"><el-tag :type="row['文件下载']?'success':'info'" size="small">{{ row['文件下载'] ? '生效' : '不适用' }}</el-tag></template></el-table-column>
          </el-table>
        </el-card>
      </el-tab-pane>

      <!-- ==================== 资源授权 ==================== -->
      <el-tab-pane label="资源授权" name="resource">
        <el-alert type="info" :closable="false" show-icon style="margin-bottom:12px"
          title="本页是策略的批量快捷视图(按资源×主体直接定等级);完整能力(动作/行谓词/环境/义务/生命周期)在「策略编辑器」配置。两者写同一策略库。"/>
        <el-row :gutter="12">
          <el-col :span="5">
            <el-card shadow="never">
              <template #header>主体</template>
              <el-radio-group v-model="subjType" size="small" style="margin-bottom:8px">
                <el-radio-button label="role">角色</el-radio-button>
                <el-radio-button label="user">用户</el-radio-button>
              </el-radio-group>
              <el-select v-model="subject" size="small" style="width:100%">
                <el-option v-for="o in subjectOptions" :key="o" :label="o" :value="o"/>
              </el-select>
            </el-card>
          </el-col>
          <el-col :span="9">
            <el-card shadow="never">
              <template #header>资源树(流 → 列 / 标签 / 文件)</template>
              <el-tree :data="resTree" :props="treeProps" node-key="key" highlight-current @node-click="onResClick" default-expand-all/>
            </el-card>
          </el-col>
          <el-col :span="10">
            <el-card shadow="never">
              <template #header>对「{{ curRes || '未选择' }}」授权</template>
              <div class="lv-row">
                <div v-for="(l, i) in levels" :key="l.k" class="lv" :class="{on: level===l.k}" @click="level=l.k">
                  <b>{{ l.k }}</b><br><span class="lv-sub">{{ l.v }}</span>
                </div>
              </div>
              <el-radio-group v-model="effect" size="small" style="margin:10px 0">
                <el-radio-button label="grant">授予</el-radio-button>
                <el-radio-button label="deny">拒绝</el-radio-button>
              </el-radio-group>
              <el-alert v-if="effect==='deny'" type="error" :closable="false" show-icon title="deny 短路:命中即拒绝,无视其他任何授权;并向下继承。"/>
              <el-button type="primary" size="small" style="margin-top:10px" @click="saveRes">保存策略</el-button>
            </el-card>
          </el-col>
        </el-row>
        <el-card shadow="never" header="该主体已有策略(稀疏矩阵:只显示非默认)">
          <el-table :data="curPolicies" size="small" border>
            <el-table-column prop="res" label="资源"/>
            <el-table-column prop="lv" label="等级" width="90"><template #default="{row}"><el-tag :type="lvTag(row.lv)" size="small">{{ row.lv }}</el-tag></template></el-table-column>
            <el-table-column prop="eff" label="效果" width="90"><template #default="{row}"><el-tag :type="row.eff==='grant'?'success':'danger'" size="small">{{ row.eff==='grant'?'授予':'拒绝' }}</el-tag></template></el-table-column>
            <el-table-column label="操作" width="80"><template #default><el-button link type="primary" size="small">编辑</el-button></template></el-table-column>
          </el-table>
        </el-card>
      </el-tab-pane>

      <!-- ==================== 策略编辑器(向导式,参考权限系统设计) ==================== -->
      <el-tab-pane label="策略编辑器" name="editor">
        <div class="wizard">
          <!-- 左:步骤导航 -->
          <aside class="wiz-nav">
            <div v-for="(st,i) in steps" :key="st.id" class="wiz-step" :class="{active:i===wStep, done:i<wStep}" @click="wStep=i">
              <div class="wiz-idx">{{ i < wStep ? '✓' : (i+1) }}</div>
              <div class="wiz-body">
                <div class="wiz-name">{{ st.name }}</div>
                <div class="wiz-desc">{{ st.desc }}</div>
              </div>
            </div>
          </aside>

          <!-- 中:表单 -->
          <main class="wiz-form">
            <template v-if="wStep===0">
              <h3 class="wiz-title">基本信息</h3>
              <div class="wiz-row">
                <div class="wiz-col">
                  <div class="fl">策略名称 *</div>
                  <el-input v-model="pName" size="small" placeholder="如:风控角色 · rt_sync_c 只读"/>
                  <div class="fl">策略描述</div>
                  <el-input v-model="pDesc" size="small" type="textarea" :rows="2"/>
                </div>
                <div class="wiz-col">
                  <div class="fl">优先级(同 specificity 冲突时大者优先,缺省 0)</div>
                  <el-input-number v-model="pPriority" size="small" :min="0" :max="99" style="width:100%"/>
                  <div class="fl">效果</div>
                  <el-radio-group v-model="pEffect" size="small">
                    <el-radio-button label="grant">grant 授予</el-radio-button>
                    <el-radio-button label="deny">deny 拒绝</el-radio-button>
                  </el-radio-group>
                </div>
              </div>
              <el-alert type="info" :closable="false" show-icon style="margin-top:8px"
                title="冲突消解三级:① specificity 链(deny 短路 → user > role > scope > 兜底)② 显式 priority ③ 仍歧义 → 发布期拒绝,运行时永不猜"/>
              <div class="fl" style="margin-top:10px">生命周期</div>
              <el-switch v-model="pDraft" active-text="draft(草稿)" inactive-text="active(立即生效)"/>
              <el-date-picker v-model="pWindow" type="datetimerange" size="small" style="width:100%;margin-top:8px"
                start-placeholder="effectiveFrom" end-placeholder="effectiveTo"/>
            </template>

            <template v-else-if="wStep===1">
              <h3 class="wiz-title">主体配置</h3>
              <div class="fl">主体类型</div>
              <el-radio-group v-model="pSubjType" size="small">
                <el-radio-button label="role">角色</el-radio-button>
                <el-radio-button label="user">用户</el-radio-button>
                <el-radio-button label="dept" disabled>部门(未启用)</el-radio-button>
              </el-radio-group>
              <div class="fl">主体</div>
              <el-select v-model="pSubj" size="small" style="width:100%">
                <el-option v-for="o in ['risk-role','analyst-role','ops-role','zhang']" :key="o" :label="o" :value="o"/>
              </el-select>
              <div class="hint2" style="margin-top:8px">主体的属性收窄(如 region)通过「行规则」中的 ${{'{subject.xxx}'}} 变量实现,不在策略层重复声明。</div>
            </template>

            <template v-else-if="wStep===2">
              <h3 class="wiz-title">资源配置</h3>
              <div class="fl">资源</div>
              <el-select v-model="pRes" size="small" style="width:100%" @change="onRes">
                <el-option label="stream: rt_sync_c" value="rt_sync_c"/>
                <el-option label="stream: customer_wide" value="customer_wide"/>
                <el-option label="file: /data/out/**" value="/data/out/**"/>
                <el-option label="tag: 业务域/风控" value="业务域/风控"/>
              </el-select>
              <div class="fl">列白名单(不勾 = 不可见;敏感列建议配掩码)</div>
              <el-table :data="colRows" size="small" border>
                <el-table-column width="46"><template #default="{row}"><el-checkbox :model-value="pCols.includes(row.name)" @change="v=>toggleCol(row.name,v)"/></template></el-table-column>
                <el-table-column prop="name" label="列名"/>
                <el-table-column prop="type" label="类型" width="90"/>
                <el-table-column label="敏感级" width="80"><template #default="{row}"><el-tag size="small" :type="['success','warning','warning','danger'][row.sens]||'success'">{{ 'L'+row.sens }}</el-tag></template></el-table-column>
              </el-table>
              <div class="hint2" style="margin-top:6px">继承语义:列级未显式声明时继承表级;标签子节点继承父节点;file 目录通配向下覆盖。</div>
            </template>

            <template v-else-if="wStep===3">
              <h3 class="wiz-title">动作与约束</h3>
              <div class="fl">动作(层级 + 通配;灰 = 定义层已登记、本项目未启用)</div>
              <div style="font-size:12px;margin:4px 0 2px"><b>read.*</b></div>
              <el-checkbox-group v-model="pActs" size="small">
                <el-checkbox label="read.snapshot"/><el-checkbox label="read.changes"/><el-checkbox label="read.query"/>
                <el-checkbox label="read.aggregate" disabled/><el-checkbox label="read.sample" disabled/>
              </el-checkbox-group>
              <div style="font-size:12px;margin:8px 0 2px"><b>export.* / download / subscribe.*</b></div>
              <el-checkbox-group v-model="pActs" size="small">
                <el-checkbox label="export.csv" disabled/><el-checkbox label="download"/>
                <el-checkbox label="subscribe.pull" disabled/>
              </el-checkbox-group>
              <div style="font-size:12px;margin:8px 0 2px"><b>write.*</b>(定义层保留;本项目进侧不启用,D5)</div>
              <el-checkbox-group size="small" disabled><el-checkbox label="write.insert"/></el-checkbox-group>

              <div class="fl" style="margin-top:12px">行谓词(内联编辑 或 引用行规则 —— 零件复用)</div>
              <el-radio-group v-model="pRowMode" size="small" style="margin-bottom:6px">
                <el-radio-button label="inline">内联编辑</el-radio-button>
                <el-radio-button label="ref">引用行规则</el-radio-button>
              </el-radio-group>
              <el-input v-if="pRowMode==='inline'" v-model="pRow" type="textarea" :rows="2" size="small" @input="renderStmt"/>
              <el-select v-else v-model="pRowRef" size="small" style="width:100%" @change="renderStmt">
                <el-option v-for="r in rowRuleOptions" :key="r.id" :label="`#${r.id} ${r.name}`" :value="r.id"/>
              </el-select>
              <div v-if="pRowMode==='ref'" class="hint2">引用规则自带的 bind_scope 随规则生效;规则变更 → 引用它的策略版本递增(收窄 &lt;1s)。</div>
              <div class="chips" v-if="pRowMode==='inline'">
                <span v-for="v in vars" :key="v" class="chip" @click="insVar(v)">${{'{'}}{{ v }}{{'}'}}</span>
              </div>
              <div class="fl">bind_scope 变量白名单(谓词用到的必须声明;未声明 → 发布拒绝)</div>
              <el-checkbox-group v-model="pBind" size="small" :disabled="pRowMode==='ref'">
                <el-checkbox label="subject.region"/><el-checkbox label="subject.username"/><el-checkbox label="env.channel"/>
              </el-checkbox-group>

              <div class="fl">标签范围 · 列脱敏 · 环境</div>
              <el-select v-model="pTag" size="small" style="width:100%">
                <el-option label="prefix: 业务域/风控" value="prefix:业务域/风控"/>
                <el-option label="全部(继承)" value="all"/>
              </el-select>
              <el-select v-model="pMask" size="small" style="width:100%;margin-top:6px">
                <el-option label="phone → MASK_PHONE" value="phone=MASK_PHONE"/>
                <el-option label="无脱敏" value=""/>
              </el-select>
              <el-select v-model="pEnvChannel" size="small" style="width:100%;margin-top:6px">
                <el-option label="channel: api" value="api"/><el-option label="channel: internal" value="internal"/>
                <el-option label="无通道约束" value=""/>
              </el-select>
              <el-input v-model="pPurpose" size="small" placeholder="purpose 用途标注(合规审计,可空)" style="margin-top:6px"/>
              <div style="margin-top:6px"><el-switch v-model="pTwEnabled" size="small" active-text="timeWindow 时间窗"/></div>
              <el-time-picker v-if="pTwEnabled" v-model="pTwRange" is-range size="small" style="width:100%;margin-top:6px"/>
              <div class="hint2" style="margin-top:6px">ipRange / deviceType / authLevel —— 定义层已登记,本项目未启用。</div>
            </template>

            <template v-else>
              <h3 class="wiz-title">义务与确认</h3>
              <div class="fl">义务 obligations(audit 必选)</div>
              <el-checkbox-group v-model="pObl" size="small">
                <el-checkbox label="audit" disabled checked/>
                <el-checkbox label="rateLimit"/>
              </el-checkbox-group>
              <div class="fl">审计级别</div>
              <el-radio-group v-model="auditLevel" size="small">
                <el-radio-button label="basic">基础</el-radio-button>
                <el-radio-button label="detail">详细</el-radio-button>
              </el-radio-group>
              <div class="fl">配额 QPS(rateLimit)</div>
              <el-input-number v-model="rateQps" size="small" :min="0" style="width:100%"/>
              <div class="fl">策略摘要</div>
              <div class="pv-json" style="background:#f0f6ff;color:#1d4ed8;border-color:#bfdbfe" v-html="summaryHtml"/>
            </template>

            <div class="wiz-actions">
              <el-button size="small" @click="saveDraft">保存草稿</el-button>
              <span style="flex:1"/>
              <el-button v-if="wStep>0" size="small" @click="wStep--">上一步</el-button>
              <el-button v-if="wStep<4" type="primary" size="small" @click="wStep++">下一步</el-button>
              <template v-else>
                <el-button size="small" @click="simulate">测试仿真</el-button>
                <el-button type="primary" size="small" @click="wizardPublish">发布</el-button>
              </template>
            </div>
          </main>

          <!-- 右:实时预览 -->
          <aside class="wiz-preview">
            <div class="pv-head">实时预览 <el-tag size="small" type="info">v1</el-tag></div>
            <el-tabs v-model="pvTab" size="small">
              <el-tab-pane label="编译产物" name="artifact">
                <div class="art"><div class="art-l">决策结果</div><div class="art-v">{{ pEffect==='deny' ? 'DENY' : 'PERMIT' }}</div></div>
                <div class="art"><div class="art-l">行级谓词(TiDB 方言)</div><div class="art-v hl">{{ artifactRow }}</div></div>
                <div class="art"><div class="art-l">绑定参数 bindOrder</div><div class="art-v">{{ artifactBinds }}</div></div>
                <div class="art"><div class="art-l">可见列</div><div class="art-v">{{ pCols.map(c=>c.split(' ')[0]).join(', ') || '—' }}</div></div>
                <div class="art"><div class="art-l">列级掩码</div><div class="art-v">{{ pMask || '无' }}</div></div>
                <div class="art"><div class="art-l">义务</div><div class="art-v">{{ pObl.join(', ') }}</div></div>
                <div class="art-l" style="margin-top:12px">编译报告(策略不声明引擎,PDP 编译预览)</div>
                <el-table :data="compileReport" size="small" border>
                  <el-table-column prop="engine" label="引擎" width="110"/>
                  <el-table-column label="下推" width="76"><template #default="{row}"><el-tag size="small" :type="row.full?'success':'warning'">{{ row.full?'全下推':'部分' }}</el-tag></template></el-table-column>
                  <el-table-column prop="note" label="说明"/>
                </el-table>
              </el-tab-pane>
              <el-tab-pane label="策略 JSON" name="json">
                <pre class="pv-json">{{ prettyJson }}</pre>
              </el-tab-pane>
              <el-tab-pane label="校验清单" name="checks">
                <div v-for="(c,i) in checks" :key="i" class="ck" :class="c.st">
                  <span class="ck-i">{{ c.st==='ok'?'✓':(c.st==='warn'?'!':'✕') }}</span>
                  <div style="flex:1"><div>{{ c.t }}</div><div class="ck-d">{{ c.d }}</div></div>
                </div>
              </el-tab-pane>
            </el-tabs>
          </aside>
        </div>
      </el-tab-pane>

      <!-- ==================== 行规则 ==================== -->
      <el-tab-pane label="行规则管理" name="rowrules">
        <el-row :gutter="12">
          <el-col :span="12">
            <el-card shadow="never" header="新建行规则">
              <el-select v-model="rrRes" size="small" style="width:100%">
                <el-option label="dataset: rt_sync_c" value="rt_sync_c"/>
                <el-option label="dataset: dw_order" value="dw_order"/>
                <el-option label="tag: 业务域/风控" value="tag"/>
              </el-select>
              <el-radio-group v-model="rrKind" size="small" style="margin:10px 0">
                <el-radio-button label="cond">cond 谓词</el-radio-button>
                <el-radio-button label="list">list 名单</el-radio-button>
                <el-radio-button label="tag_ref">tag_ref 标签引用</el-radio-button>
              </el-radio-group>
              <el-input v-model="rrExpr" type="textarea" :rows="2" size="small" placeholder="owner = ${subject.username}"/>
              <div class="chips"><span v-for="v in vars" :key="v" class="chip" @click="rrExpr += '${'+v+'}'">${{'{'}}{{ v }}{{'}'}}</span></div>
              <div class="fl">bind_scope 白名单</div>
              <el-checkbox-group v-model="rrBind" size="small"><el-checkbox label="subject.username"/></el-checkbox-group>
              <div style="margin-top:10px"><el-button type="primary" size="small">保存规则</el-button></div>
            </el-card>
          </el-col>
          <el-col :span="12">
            <el-card shadow="never" header="生效点(如实标注,不配不生效的策略)">
              <el-table :data="rrPoints" size="small" border>
                <el-table-column prop="outlet" label="出口"/>
                <el-table-column label="状态" width="120"><template #default="{row}"><el-tag :type="row.ok?'success':'info'" size="small">{{ row.ok?'生效':'阶段二' }}</el-tag></template></el-table-column>
              </el-table>
              <el-alert type="warning" :closable="false" show-icon style="margin-top:8px" title="tag_ref 型:标签变更触发策略版本递增,收窄 <1s 生效。"/>
            </el-card>
          </el-col>
        </el-row>
        <el-card shadow="never" header="规则列表">
          <el-table :data="rrList" size="small" border>
            <el-table-column prop="name" label="规则"/>
            <el-table-column prop="res" label="资源"/>
            <el-table-column prop="kind" label="类型" width="90"/>
            <el-table-column prop="expr" label="谓词"/>
            <el-table-column prop="bind" label="变量白名单" width="150"/>
          </el-table>
        </el-card>
      </el-tab-pane>

      <!-- ==================== 契约 ==================== -->
      <el-tab-pane label="契约管理" name="contracts">
        <el-card shadow="never" header="契约列表(入口合一,数据仍在 contract 表)">
          <el-table :data="contracts" size="small" border>
            <el-table-column prop="id" label="契约" width="160"/>
            <el-table-column prop="consumer" label="消费方" width="110"/>
            <el-table-column prop="stream" label="覆盖流" width="110"/>
            <el-table-column prop="cols" label="列白名单"/>
            <el-table-column prop="maxLevel" label="密级上限" width="80"/>
            <el-table-column prop="rowCond" label="行条件" width="130"/>
            <el-table-column prop="channel" label="通道" width="90"/>
            <el-table-column label="操作" width="80"><template #default="{row}"><el-button link type="primary" size="small" @click="showContract(row)">预览</el-button></template></el-table-column>
          </el-table>
        </el-card>
        <el-card v-if="contractView" shadow="never" header="契约视图预览(合算后 —— 下发给消费方的最终形态)">
          <pre class="stmt">{{ contractView }}</pre>
        </el-card>
      </el-tab-pane>

      <!-- ==================== 密级与脱敏 ==================== -->
      <el-tab-pane label="密级与脱敏" name="mask">
        <el-alert type="info" :closable="false" show-icon style="margin-bottom:12px"
          title="本页是参考数据与运维页:敏感级字典为平台常量(L0-L3 固定,几乎不变);脱敏注册表由开发/运维登记实现,日常配置在策略编辑器中「选用」规则。"/>
        <el-card shadow="never" header="敏感级字典(0-3)—— 平台常量">
          <el-table :data="levelDict" size="small" border>
            <el-table-column prop="lv" label="级" width="90"/>
            <el-table-column prop="name" label="名称" width="140"/>
            <el-table-column prop="desc" label="含义"/>
          </el-table>
        </el-card>
        <el-card shadow="never" header="脱敏规则注册表(mask_rule;两端同源,版本不一致拒下发)">
          <template #header>
            <div style="display:flex;justify-content:space-between;align-items:center">
              <span>脱敏规则注册表(mask_rule;两端同源,版本不一致拒下发)</span>
              <el-button type="primary" size="small" @click="maskDialog=true">+ 注册新规则</el-button>
            </div>
          </template>
          <el-table :data="maskRules" size="small" border>
            <el-table-column prop="code" label="编码" width="140"/>
            <el-table-column prop="param" label="参数" width="140"/>
            <el-table-column prop="ver" label="实现版本" width="90"/>
            <el-table-column label="平台侧" width="90"><template #default><el-tag type="success" size="small">v{{ '3' }}</el-tag></template></el-table-column>
            <el-table-column label="计算面" width="110"><template #default="{row}"><el-tag :type="row.sync?'success':'warning'" size="small">{{ row.sync ? '已同步' : '待同步' }}</el-tag></template></el-table-column>
          </el-table>
        </el-card>
        <el-dialog v-model="maskDialog" title="注册脱敏规则(运维操作)" width="420px">
          <el-form label-width="90px" size="small">
            <el-form-item label="规则编码"><el-input v-model="newMask.code" placeholder="如 MASK_EMAIL"/></el-form-item>
            <el-form-item label="参数"><el-input v-model="newMask.param" placeholder="如 保留前2后4"/></el-form-item>
            <el-form-item label="实现版本"><el-input v-model="newMask.ver" placeholder="v1"/></el-form-item>
          </el-form>
          <div class="hint2">注册后需在平台侧与计算面同步实现;两端版本不一致时,含该规则的策略下发会被拒(fail-closed)。</div>
          <template #footer>
            <el-button size="small" @click="maskDialog=false">取消</el-button>
            <el-button type="primary" size="small" @click="registerMask">注册</el-button>
          </template>
        </el-dialog>
      </el-tab-pane>

      <!-- ==================== 有效权限预览 ==================== -->
      <el-tab-pane label="有效权限预览" name="preview">
        <el-row :gutter="12">
          <el-col :span="5">
            <el-card shadow="never" header="查询条件">
              <el-select v-model="pvSubj" size="small" style="width:100%;margin-bottom:8px" @change="renderPreview">
                <el-option label="risk-role(角色)" value="risk-role"/>
                <el-option label="zhang(用户)" value="zhang"/>
              </el-select>
              <el-select v-model="pvRes" size="small" style="width:100%;margin-bottom:8px" @change="renderPreview">
                <el-option label="stream: rt_sync_c" value="rt_sync_c"/>
                <el-option label="stream: dw_order" value="dw_order"/>
              </el-select>
              <el-select v-model="pvAct" size="small" style="width:100%" @change="renderPreview">
                <el-option label="read.snapshot" value="snapshot"/>
                <el-option label="read.changes" value="changes"/>
              </el-select>
            </el-card>
          </el-col>
          <el-col :span="9">
            <el-card shadow="never" header="最终结果(判定合成)">
              <el-table :data="pvResult" size="small" border>
                <el-table-column prop="k" label="项" width="110"/>
                <el-table-column label="结果"><template #default="{row}"><span v-html="row.v"/></template></el-table-column>
              </el-table>
            </el-card>
          </el-col>
          <el-col :span="10">
            <el-card shadow="never" header="为什么 —— 判定链解释">
              <div v-for="(w,i) in pvWhy" :key="i" class="why">
                <div class="why-t">{{ w.t }}</div>
                <div class="why-d">{{ w.d }}</div>
              </div>
            </el-card>
          </el-col>
        </el-row>
      </el-tab-pane>

      <!-- ==================== 变更审计 ==================== -->
      <el-tab-pane label="变更审计" name="audit">
        <el-card shadow="never" header="策略变更历史(谁在何时把什么改成了什么)">
          <el-table :data="audits" size="small" border>
            <el-table-column prop="time" label="时间" width="120"/>
            <el-table-column prop="who" label="操作人" width="90"/>
            <el-table-column label="动作" width="90"><template #default="{row}"><el-tag :type="row.act==='删除'?'danger':row.act==='拒绝'?'warning':'success'" size="small">{{ row.act }}</el-tag></template></el-table-column>
            <el-table-column prop="obj" label="对象" width="150"/>
            <el-table-column prop="delta" label="变更前 → 变更后"/>
          </el-table>
        </el-card>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useRoute, useRouter } from 'vue-router'
// TODO: 接入后端 API —— 本页全部为 mock 数据,接口契约见《数据权限统一控制模型设计》§8
const route = useRoute()
const router = useRouter()
const tab = ref('overview')
const TAB_NAMES = ['overview', 'resource', 'editor', 'rowrules', 'contracts', 'mask', 'preview', 'audit']
// 子菜单 → tab 联动:路由参数直达对应 tab;tab 切换同步 URL
watch(() => route.params.page, p => { const v = p ? String(p) : 'overview'; if (TAB_NAMES.includes(v)) tab.value = v })
onMounted(() => { const p = route.params.page; if (p && TAB_NAMES.includes(String(p))) tab.value = String(p) })
const onTabChange = name => router.replace('/perm-center/' + name)

/* ---------- 总览 ---------- */
const stats = [
  { n: '128', label: '生效策略(资源级 96 · 行规则 24 · 脱敏 8)' },
  { n: '41', label: '覆盖资源(流 18 / 文件 3 / 标签 20)' },
  { n: '6 / 6', label: '出口已挂 PEP(全部生效)' },
  { n: '2', label: '待处理:收窄事件延迟 > 5s', color: '#d97706' },
]
const matrix = [
  { kind: '资源级(列/密级)', '平台 REST': 1, 'v2 计算面': 1, 'Spark 批': 1, 'Flink 流': 1, '订阅 PULL': 1, '文件下载': 1 },
  { kind: '行规则 cond', '平台 REST': 1, 'v2 计算面': 1, 'Spark 批': 1, 'Flink 流': 1, '订阅 PULL': 1, '文件下载': 0 },
  { kind: '行规则 tag_ref', '平台 REST': 1, 'v2 计算面': 0, 'Spark 批': 0, 'Flink 流': 0, '订阅 PULL': 1, '文件下载': 0 },
]

/* ---------- 资源授权 ---------- */
const subjType = ref('role')
const subject = ref('risk-role')
const subjectOptions = computed(() =>
  subjType.value === 'role' ? ['risk-role', 'analyst-role', 'ops-role'] : ['zhang', 'li'])
const resTree = [
  { key: 'rt_sync_c', label: '📊 rt_sync_c', children: [
    { key: 'rt_sync_c.id', label: 'id (key)' }, { key: 'rt_sync_c.v', label: 'v' },
    { key: 'rt_sync_c.seq', label: 'seq' }, { key: 'rt_sync_c.phone', label: 'phone' }, { key: 'rt_sync_c._commit_ts', label: '_commit_ts' } ] },
  { key: 'customer_wide', label: '📊 customer_wide', children: [ { key: 'customer_wide.name', label: 'name' }, { key: 'customer_wide.phone', label: 'phone' } ] },
  { key: '/data/out/**', label: '📁 /data/out/**' },
  { key: '业务域/风控/黑名单', label: '🏷️ 业务域/风控/黑名单' },
]
const treeProps = { label: 'label' }
const curRes = ref('rt_sync_c')
const level = ref('L3')
const effect = ref('grant')
const levels = [ { k: 'L0', v: '不可见' }, { k: 'L1', v: '元数据' }, { k: 'L2', v: '脱敏' }, { k: 'L3', v: '全量' } ]
const onResClick = n => { if (!n.children) curRes.value = n.key }
const lvTag = l => ({ L0: 'danger', L1: 'warning', L2: 'warning', L3: 'success' }[l])
const policiesMap = {
  'risk-role': [ { res: 'stream: rt_sync_c', lv: 'L2', eff: 'grant' }, { res: 'stream: dw_order', lv: 'L3', eff: 'grant' }, { res: 'tag: 密级/机密', lv: 'L0', eff: 'deny' } ],
  'analyst-role': [ { res: 'stream: customer_wide', lv: 'L2', eff: 'grant' } ],
  'ops-role': [ { res: 'file: /data/out/**', lv: 'L3', eff: 'grant' } ],
  zhang: [ { res: 'stream: rt_sync_c', lv: 'L3', eff: 'grant' } ],
  li: [],
}
const curPolicies = computed(() => policiesMap[subject.value] || [])
const saveRes = () => ElMessage.success('已保存(原型):策略写库 → 版本号 +1 → 收窄事件广播')

/* ---------- 策略编辑器(向导式) ---------- */
const wStep = ref(0)
const pvTab = ref('artifact')
const steps = [
  { id: 'basic', name: '基本信息', desc: '名称、优先级、效果、生命周期' },
  { id: 'subject', name: '主体配置', desc: '角色 / 用户' },
  { id: 'resource', name: '资源配置', desc: '资源 + 列白名单' },
  { id: 'constraints', name: '动作与约束', desc: '动作 + 行谓词 + 掩码 + 环境' },
  { id: 'obligations', name: '义务与确认', desc: '审计 + 配额 + 摘要' },
]
const pName = ref('风控角色 · rt_sync_c 只读')
const pDesc = ref('风控角色只能看本区域已支付订单,phone 脱敏')
const pEffect = ref('grant')
const pSubjType = ref('role')
const pSubj = ref('risk-role')
const pPriority = ref(0)
const pRes = ref('rt_sync_c')
const pActs = ref(['read.snapshot', 'read.changes'])
const pCols = ref(['id (key)', 'v', 'seq'])
const pRowMode = ref('inline')
const pRowRef = ref(24)
const rowRuleOptions = [
  { id: 24, name: '仅本人订单(owner = ${subject.username})' },
  { id: 17, name: '华东白名单(region IN 华东,华北)' },
  { id: 9, name: '风控可见域(tag IN 业务域/风控/**)' },
]
const pRow = ref("status = 'paid' AND region = ${subject.region}")
const pBind = ref(['subject.region'])
const pTag = ref('prefix:业务域/风控')
const pMask = ref('phone=MASK_PHONE')
const pEnvChannel = ref('api')
const pPurpose = ref('')
const pTwEnabled = ref(false)
const pTwRange = ref(null)
const pObl = ref(['audit'])
const auditLevel = ref('detail')
const rateQps = ref(100)
const pDraft = ref(false)
const pWindow = ref(null)
const vars = ['subject.username', 'subject.region', 'env.channel', 'env.time']
const insVar = v => { pRow.value += '${' + v + '}' }
const colRows = [
  { name: 'id (key)', type: 'bigint', sens: 0 },
  { name: 'v', type: 'varchar', sens: 0 },
  { name: 'seq', type: 'bigint', sens: 1 },
  { name: 'phone', type: 'varchar', sens: 2 },
  { name: 'region', type: 'varchar', sens: 1 },
  { name: 'status', type: 'varchar', sens: 0 },
]
const toggleCol = (name, v) => {
  const arr = pCols.value
  const i = arr.indexOf(name)
  if (v && i < 0) arr.push(name)
  else if (!v && i >= 0) arr.splice(i, 1)
}
const stmtErr = ref('')
const stmt = ref('')
const renderStmt = () => {
  const used = [...pRow.value.matchAll(/\$\{([^}]+)\}/g)].map(m => m[1])
  const miss = pRowMode.value === 'inline' ? used.filter(v => !pBind.value.includes(v)) : []
  stmtErr.value = miss.length ? '校验失败:谓词变量 ' + miss.join(', ') + ' 未在 bind_scope 声明 —— 策略拒绝发布(fail-closed)。' : ''
}
const artifactRow = computed(() => {
  const src = pRowMode.value === 'ref'
    ? rowRuleOptions.find(r => r.id === pRowRef.value)?.name || ''
    : pRow.value
  const rendered = src.replace(/\$\{([^}]+)\}/g, '?')
  return rendered === '?' ? rendered : rendered || '—'
})
const artifactBinds = computed(() => {
  const src = pRowMode.value === 'ref' ? '' : pRow.value
  const binds = [...src.matchAll(/\$\{([^}]+)\}/g)].map((m, i) => `[${i}] ${m[1]}`)
  return binds.join('\n') || '无'
})
const prettyJson = computed(() => {
  const rowPart = pRowMode.value === 'ref'
    ? { mode: 'ref', ruleRef: 'rr-' + pRowRef.value, expr: null }
    : { mode: 'inline', ruleRef: null, expr: pRow.value }
  const env = {}
  if (pEnvChannel.value) env.channel = pEnvChannel.value
  if (pPurpose.value) env.purpose = pPurpose.value
  if (pTwEnabled.value) env.timeWindow = 'enabled'
  return JSON.stringify({
    policyId: 'pol-new', policyName: pName.value, description: pDesc.value,
    version: 1, status: pDraft.value ? 'draft' : 'active', priority: pPriority.value,
    effect: pEffect.value, effectiveFrom: null, effectiveTo: null,
    subject: { type: pSubjType.value, id: pSubj.value },
    resource: { type: 'stream', path: pRes.value },
    actions: pActs.value,
    constraints: {
      columns: pCols.value.map(c => c.split(' ')[0]),
      row: { ...rowPart, bindScope: pBind.value },
      tagScope: pTag.value === 'all' ? { mode: 'all', path: null } : { mode: 'prefix', path: pTag.value.split(':')[1] },
      maskRules: pMask.value ? { [pMask.value.split('=')[0]]: pMask.value.split('=')[1] } : {},
      env,
    },
    obligations: [{ name: 'audit', level: auditLevel.value }, ...(pObl.value.includes('rateLimit') ? [{ name: 'rateLimit', params: { qps: rateQps.value } }] : [])],
  }, null, 2)
})
const checks = computed(() => {
  const list = []
  list.push(pName.value ? { st: 'ok', t: '策略名称', d: pName.value } : { st: 'err', t: '策略名称', d: '必填' })
  list.push(pSubj.value ? { st: 'ok', t: '主体配置', d: pSubj.value } : { st: 'err', t: '主体配置', d: '至少选择一个主体' })
  list.push(pRes.value ? { st: 'ok', t: '资源配置', d: pRes.value } : { st: 'err', t: '资源配置', d: '至少选择一个资源' })
  list.push(pActs.value.length ? { st: 'ok', t: '动作选择', d: pActs.value.join(', ') } : { st: 'err', t: '动作选择', d: '至少选择一个动作' })
  list.push(pRow.value || pRowMode.value === 'ref' ? { st: 'ok', t: '行级表达式', d: pRowMode.value === 'ref' ? '引用 #' + pRowRef.value : '语法校验通过' } : { st: 'warn', t: '行级表达式', d: '未配置(可选)' })
  const usedVars = [...pRow.value.matchAll(/\$\{([^}]+)\}/g)].map(m => m[1])
  list.push({ st: usedVars.every(v => pBind.value.includes(v)) ? 'ok' : 'err', t: '变量声明 bind_scope', d: usedVars.length ? usedVars.join(', ') : '无变量引用' })
  list.push(pMask.value ? { st: 'ok', t: '列级掩码', d: pMask.value } : { st: 'warn', t: '列级掩码', d: '未配置(可选)' })
  const sensExposed = pCols.value.filter(c => colRows.find(r => r.name === c && r.sens >= 2))
  const sensMasked = sensExposed.every(c => pMask.value.includes(c.split(' ')[0]))
  list.push(sensExposed.length === 0 ? { st: 'ok', t: '敏感列处理', d: '无敏感列暴露' } : (sensMasked ? { st: 'ok', t: '敏感列处理', d: '已掩码:' + sensExposed.join(', ') } : { st: 'warn', t: '敏感列处理', d: '敏感列未掩码:' + sensExposed.join(', ') }))
  list.push(stmtErr.value ? { st: 'err', t: '发布校验', d: stmtErr.value } : { st: 'ok', t: '发布校验', d: '变量已声明;发布后收窄 <1s 生效' })
  list.push({ st: 'ok', t: '义务配置', d: pObl.value.join(', ') + ' · audit ' + auditLevel.value })
  return list
})
const summaryHtml = computed(() =>
  `<b>${pName.value}</b><br>效果:${pEffect.value} · 主体:${pSubj.value} · 资源:${pRes.value}<br>` +
  `动作:${pActs.value.join(', ')}<br>行级:${pRowMode.value === 'ref' ? '引用 #' + pRowRef.value : pRow.value}<br>` +
  `优先级:${pPriority.value} · 生命周期:${pDraft.value ? 'draft' : 'active'} · 义务:${pObl.value.join(', ')}`)
const saveDraft = () => ElMessage.success('草稿已保存(原型):draft 状态不参与判定')
const simulate = () => {
  renderStmt()
  if (stmtErr.value) { ElMessage.error('测试仿真被拒:' + stmtErr.value); return }
  ElMessage.success('测试仿真(原型):PERMIT · 行级谓词已渲染 · 敏感列已掩码 · 见右侧编译产物')
}
const wizardPublish = () => {
  renderStmt()
  if (stmtErr.value) { ElMessage.error('发布被拒(校验清单):' + stmtErr.value); return }
  ElMessage.success('已发布(原型):策略写库 → 版本号+1 → 失效事件广播 → 各 PEP 本地缓存失效')
}

/* ---------- 行规则 ---------- */
const rrRes = ref('rt_sync_c')
const rrKind = ref('cond')
const rrExpr = ref('owner = ${subject.username}')
const rrBind = ref(['subject.username'])
const rrPoints = [
  { outlet: '平台 REST', ok: true }, { outlet: 'v2 计算面', ok: true },
  { outlet: 'Spark / Flink', ok: false }, { outlet: '订阅 PULL', ok: true },
]
const rrList = [
  { name: '仅本人订单', res: 'dw_order', kind: 'cond', expr: 'owner = ${subject.username}', bind: 'subject.username' },
  { name: '华东白名单', res: 'rt_sync_c', kind: 'list', expr: 'region IN (华东,华北)', bind: '—' },
  { name: '风控标签可见', res: 'tag', kind: 'tag_ref', expr: 'tag IN (业务域/风控/**)', bind: '—' },
]

/* ---------- 契约 ---------- */
const contracts = [
  { id: 'ct-sub-rt_sync_c', consumer: 'demo-sub', stream: 'rt_sync_c', cols: 'id, v', maxLevel: 2, rowCond: '—', channel: 'api' },
  { id: 'ct-tag-rt_sync_c', consumer: 'tag-platform', stream: 'rt_sync_c', cols: 'id, v, seq', maxLevel: 3, rowCond: '—', channel: 'internal' },
  { id: 'ct-risk-dw_order', consumer: 'risk-team', stream: 'dw_order', cols: 'order_id, amount, status, phone', maxLevel: 2, rowCond: "status='paid'", channel: 'api' },
]
const contractView = ref('')
const showContract = r => {
  contractView.value = `contractId    = ${r.id}\nconsumerId    = ${r.consumer}\nstream        = ${r.stream}\nallowedColumns = ${r.cols}\nmaxLevel      = ${r.maxLevel}\nrowCondition  = ${r.rowCond}\nchannel       = ${r.channel}\n# 下发协议(扁平键值,可 curl);契约 ∩ 主体后为最终视图`
}

/* ---------- 密级与脱敏 ---------- */
const maskDialog = ref(false)
const newMask = ref({ code: '', param: '', ver: 'v1' })
const registerMask = () => {
  if (!newMask.value.code) { ElMessage.warning('请填写规则编码'); return }
  maskRules.push({ code: newMask.value.code, param: newMask.value.param || '—', ver: newMask.value.ver, sync: false })
  maskDialog.value = false
  ElMessage.success('已注册(原型):两端同步实现后可被策略选用;未同步前含该规则的策略拒下发')
}
const levelDict = [
  { lv: 'L0 HIDDEN', name: '不可见', desc: '显式授权才可见;未授权即隐藏' },
  { lv: 'L1 META', name: '元数据', desc: '可见结构,不可见数据' },
  { lv: 'L2 MASKED', name: '脱敏可见', desc: '按规则脱敏后可见' },
  { lv: 'L3 FULL', name: '全量', desc: '完全可见' },
]
const maskRules = [
  { code: 'MASK_PHONE', param: '保留前3后2', ver: 'v3', sync: true },
  { code: 'MASK_FULL', param: '—', ver: 'v1', sync: true },
  { code: 'MASK_ID_CARD', param: '保留前4后4', ver: 'v2', sync: false },
]

/* ---------- 有效权限预览 ---------- */
const pvSubj = ref('risk-role')
const pvRes = ref('rt_sync_c')
const pvAct = ref('snapshot')
const pvResult = ref([])
const pvWhy = ref([])
const renderPreview = () => {
  const full = pvSubj.value === 'zhang' && pvRes.value === 'rt_sync_c'
  if (full) {
    pvResult.value = [
      { k: '最终等级', v: '<span class="tag-l3">L3 全量</span>' },
      { k: '可见列', v: 'id, v, seq, phone, _commit_ts' },
      { k: '行规则', v: '无' },
      { k: '脱敏', v: '无' },
      { k: '标签', v: '全部(继承)' },
    ]
    pvWhy.value = [
      { t: '① 契约', d: 'ct-tag-rt_sync_c 覆盖该流,grant 全部 5 列(通过)' },
      { t: '② 主体显式(user)', d: '张工 对该流有 user 级 L3 授权 —— 覆盖 role 级' },
      { t: '③ scope 前缀', d: '无更细限制(通过)' },
      { t: '④ 敏感级兜底', d: 'phone 敏感级 2 ≤ L3(通过)' },
      { t: '⑤ 结论', d: 'L3 全量。phone 敏感级 2,若主体降级到 L2 将触发脱敏。' },
    ]
  } else {
    pvResult.value = [
      { k: '最终等级', v: '<span class="tag-l2">L2 脱敏</span>' },
      { k: '可见列', v: 'id, v, seq' },
      { k: '行规则', v: "status='paid' AND region=${subject.region}" },
      { k: '脱敏', v: 'phone → MASK_PHONE' },
      { k: '标签', v: 'prefix: 业务域/风控' },
    ]
    pvWhy.value = [
      { t: '① 契约', d: 'ct-sub-rt_sync_c:列白名单 [id,v] ∩ 密级上限 2(收窄)' },
      { t: '② 主体 role', d: 'risk-role 对该流 L2 —— 列交集 = 契约∩主体 = [id,v,seq]' },
      { t: '③ scope 前缀', d: '标签“业务域/风控”前缀继承 L2(收窄)' },
      { t: '④ 行规则', d: '契约行条件 + 主体行规则 —— AND 合并' },
      { t: '⑤ 脱敏', d: 'phone 敏感级 2 → MASK_PHONE' },
      { t: '⑥ 结论', d: 'L2 脱敏可见;越权列(如 _commit_ts)连 key 都不出现。' },
    ]
  }
}
renderPreview()

/* ---------- 审计 ---------- */
const audits = [
  { time: '09-16 20:11', who: 'admin', act: '删除', obj: '策略 #312', delta: 'risk-role × rt_sync_c: L3 → (已删)' },
  { time: '09-16 19:58', who: 'zhang', act: '更新', obj: '行规则 #24', delta: "owner = ${subject.username} → 增加 AND status != 'closed'" },
  { time: '09-16 19:40', who: 'admin', act: '创建', obj: '契约 ct-tag-rt_sync_c', delta: '(新建) → 列 [id,v,seq] · 密级 3 · internal' },
  { time: '09-16 15:02', who: 'admin', act: '拒绝', obj: '策略 #298', delta: 'ops-role × 密级/机密/** 由 grant 改 deny' },
]
</script>

<style scoped>
.perm-center { padding: 12px 16px; }
.pc-tabs :deep(.el-card__header) { font-weight: 600; padding: 10px 14px; }
.stat-card { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 14px; }
.stat-n { font-size: 24px; font-weight: 700; }
.stat-l { color: #64748b; font-size: 12px; margin-top: 2px; }
.fl { color: #64748b; font-size: 12px; margin: 10px 0 4px; }
.lv-row { display: flex; gap: 6px; }
.lv { flex: 1; text-align: center; padding: 8px 4px; border: 1px solid #e2e8f0; border-radius: 7px; cursor: pointer; font-size: 12px; background: #fff; }
.lv.on { outline: 2px solid #2563eb; }
.lv-sub { font-size: 10px; color: #64748b; }
.chips { margin: 6px 0; }
.chip { display: inline-block; padding: 3px 9px; border-radius: 6px; background: #ede9fe; color: #6d28d9; font-size: 11.5px; cursor: pointer; border: 1px solid #ddd6fe; margin-right: 4px; }
.stmt { font-family: ui-monospace, Menlo, monospace; font-size: 12px; background: #0f172a; color: #e2e8f0; border-radius: 8px; padding: 12px 14px; line-height: 1.7; white-space: pre-wrap; margin: 0; }
.why { border-left: 3px solid #16a34a; padding: 8px 12px; margin: 8px 0; background: #f0fdf4; border-radius: 0 8px 8px 0; }
.why-t { font-weight: 600; font-size: 12.5px; }
.why-d { font-size: 12px; color: #64748b; margin-top: 2px; }
.hint2 { font-size: 11.5px; color: #64748b; margin-top: 4px; line-height: 1.6; }
/* 向导式策略编辑器 */
.wizard { display: grid; grid-template-columns: 190px 1fr 340px; gap: 12px; min-height: 520px; }
.wiz-nav { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 14px 0; }
.wiz-step { display: flex; gap: 10px; padding: 12px 16px; cursor: pointer; }
.wiz-step:hover { background: #f8fafc; }
.wiz-step.active { background: #eff6ff; }
.wiz-idx { width: 22px; height: 22px; border-radius: 50%; border: 2px solid #cbd5e1; display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 600; color: #64748b; flex-shrink: 0; }
.wiz-step.active .wiz-idx { background: #2563eb; border-color: #2563eb; color: #fff; }
.wiz-step.done .wiz-idx { background: #16a34a; border-color: #16a34a; color: #fff; }
.wiz-name { font-size: 13px; font-weight: 500; color: #1e293b; }
.wiz-step.active .wiz-name { color: #2563eb; }
.wiz-desc { font-size: 11px; color: #94a3b8; margin-top: 2px; }
.wiz-form { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 20px 22px; display: flex; flex-direction: column; }
.wiz-title { font-size: 16px; font-weight: 650; margin-bottom: 14px; }
.wiz-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.wiz-col { display: flex; flex-direction: column; }
.wiz-actions { display: flex; align-items: center; gap: 8px; margin-top: auto; padding-top: 16px; border-top: 1px solid #e2e8f0; }
.wiz-preview { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px 12px; overflow: auto; }
.pv-head { display: flex; justify-content: space-between; align-items: center; font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.art { margin-bottom: 8px; }
.art-l { font-size: 10.5px; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: .4px; margin-bottom: 3px; }
.art-v { font-family: ui-monospace, Menlo, monospace; font-size: 11.5px; color: #1e293b; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 6px 8px; word-break: break-all; white-space: pre-wrap; }
.art-v.hl { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
.pv-json { font-family: ui-monospace, Menlo, monospace; font-size: 11px; background: #0f172a; color: #e2e8f0; border-radius: 8px; padding: 10px 12px; line-height: 1.6; white-space: pre-wrap; margin: 0; max-height: 480px; overflow: auto; }
.ck { display: flex; gap: 8px; padding: 7px 0; border-bottom: 1px solid #f1f5f9; font-size: 12.5px; }
.ck:last-child { border-bottom: none; }
.ck-i { width: 16px; height: 16px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 10px; flex-shrink: 0; margin-top: 2px; }
.ck.ok .ck-i { background: #f0fdf4; color: #16a34a; }
.ck.warn .ck-i { background: #fffbeb; color: #d97706; }
.ck.err .ck-i { background: #fef2f2; color: #dc2626; }
.ck-d { font-size: 11px; color: #94a3b8; margin-top: 1px; word-break: break-all; }
:deep(.tag-l2) { background: #fef9c3; color: #a16207; padding: 2px 8px; border-radius: 99px; font-size: 11px; font-weight: 600; }
:deep(.tag-l3) { background: #dcfce7; color: #15803d; padding: 2px 8px; border-radius: 99px; font-size: 11px; font-weight: 600; }
</style>

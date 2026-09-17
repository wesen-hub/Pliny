<template>
  <div class="perm-center">
    <el-tabs v-model="tab" class="pc-tabs" @tab-change="onTabChange">
      <!-- ==================== 策略总览 ==================== -->
      <el-tab-pane label="策略总览" name="overview">
        <div class="page-head">
          <h2 class="page-title">策略总览</h2>
          <span class="page-sub">授权态势一眼看全:策略、资源、生效点、异常</span>
        </div>
        <div class="stat-grid">
          <div v-for="s in stats" :key="s.label" class="stat-card">
            <div class="stat-n" :style="{color:s.color||''}">{{ s.n }}</div>
            <div class="stat-l">{{ s.label }}</div>
          </div>
        </div>
        <section class="card">
          <h3 class="sec-title">生效点矩阵(策略 → 各出口是否已下发生效)</h3>
          <table class="data-table">
            <thead><tr><th>策略类</th><th>平台 REST</th><th>v2 计算面</th><th>Spark 批</th><th>Flink 流</th><th>订阅 PULL</th><th>文件下载</th></tr></thead>
            <tbody>
              <tr v-for="r in matrix" :key="r.kind">
                <td>{{ r.kind }}</td>
                <td v-for="k in ['平台 REST','v2 计算面','Spark 批','Flink 流','订阅 PULL','文件下载']" :key="k">
                  <span class="badge" :class="r[k] ? 'b-ok' : 'b-off'">{{ r[k] ? '生效' : (r[k]===false?'不适用':'阶段二') }}</span>
                </td>
              </tr>
            </tbody>
          </table>
          <p class="hint">“不适用”= 该出口无对应资源语义;“阶段二”= 迁移排期。管理面如实标注,不配不生效的策略。</p>
        </section>
        <section class="card">
          <h3 class="sec-title">最近变更</h3>
          <div v-for="a in audits.slice(0,3)" :key="a.time" class="tl-item">
            <span class="badge" :class="a.act==='删除'?'b-deny':(a.act==='拒绝'?'b-deny':'b-ok')">{{ a.act }}</span>
            <span class="tl-main">{{ a.obj }}<span class="tl-sub">{{ a.delta }}</span></span>
            <span class="tl-time">{{ a.who }} · {{ a.time }}</span>
          </div>
        </section>
      </el-tab-pane>

      <!-- ==================== 资源授权 ==================== -->
      <el-tab-pane label="资源授权" name="resource">
        <div class="page-head">
          <h2 class="page-title">资源授权</h2>
          <span class="page-sub">策略的批量快捷视图(与策略编辑器写同一策略库;完整能力在编辑器)</span>
        </div>
        <div class="tri-col">
          <aside class="card">
            <h3 class="sec-title">主体</h3>
            <el-radio-group v-model="subjType" size="small" style="margin-bottom:8px">
              <el-radio-button label="role">角色</el-radio-button>
              <el-radio-button label="user">用户</el-radio-button>
            </el-radio-group>
            <div class="selector-box">
              <label v-for="o in subjectOptions" :key="o" class="selector-item" :class="{selected: subject===o}">
                <input type="radio" name="subj" :checked="subject===o" @change="subject=o">
                <span>{{ o }}</span>
              </label>
            </div>
          </aside>
          <aside class="card">
            <h3 class="sec-title">资源树(流 → 列 / 标签 / 文件)</h3>
            <el-tree :data="resTree" :props="treeProps" node-key="key" highlight-current @node-click="onResClick" default-expand-all/>
          </aside>
          <aside class="card">
            <h3 class="sec-title">对「{{ curRes }}」授权</h3>
            <div class="lv-row">
              <div v-for="l in levels" :key="l.k" class="lv" :class="{on: level===l.k}" @click="level=l.k">
                <b>{{ l.k }}</b><span class="lv-sub">{{ l.v }}</span>
              </div>
            </div>
            <el-radio-group v-model="effect" size="small" style="margin:10px 0">
              <el-radio-button label="grant">授予</el-radio-button>
              <el-radio-button label="deny">拒绝</el-radio-button>
            </el-radio-group>
            <div v-if="effect==='deny'" class="alert a-err">deny 短路:命中即拒绝,无视其他授权;并向下继承。</div>
            <el-button type="primary" size="small" @click="saveRes">保存策略</el-button>
          </aside>
        </div>
        <section class="card">
          <h3 class="sec-title">该主体已有策略(稀疏矩阵:只显示非默认)</h3>
          <table class="data-table">
            <thead><tr><th>资源</th><th>等级</th><th>效果</th><th style="width:70px"></th></tr></thead>
            <tbody>
              <tr v-for="p in curPolicies" :key="p.res">
                <td>{{ p.res }}</td>
                <td><span class="badge" :class="'b-l'+p.lv[1]">{{ p.lv }}</span></td>
                <td><span class="badge" :class="p.eff==='grant'?'b-ok':'b-deny'">{{ p.eff==='grant'?'授予':'拒绝' }}</span></td>
                <td><el-button link type="primary" size="small">编辑</el-button></td>
              </tr>
            </tbody>
          </table>
        </section>
      </el-tab-pane>

      <!-- ==================== 策略编辑器(五步向导) ==================== -->
      <el-tab-pane label="策略编辑器" name="editor">
        <div class="wizard">
          <aside class="wiz-nav">
            <div v-for="(st,i) in steps" :key="st.id" class="wiz-step" :class="{active:i===wStep, done:i<wStep}" @click="wStep=i">
              <div class="wiz-idx">{{ i < wStep ? '✓' : (i+1) }}</div>
              <div class="wiz-body">
                <div class="wiz-name">{{ st.name }}</div>
                <div class="wiz-desc">{{ st.desc }}</div>
              </div>
            </div>
          </aside>
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
              <div class="alert a-info" style="margin-top:10px">冲突消解三级:① specificity 链(deny 短路 → user > role > scope > 兜底)② 显式 priority ③ 仍歧义 → 发布期拒绝,运行时永不猜</div>
              <div class="fl" style="margin-top:10px">生命周期</div>
              <el-switch v-model="pDraft" active-text="draft(草稿)" inactive-text="active(立即生效)"/>
              <el-date-picker v-model="pWindow" type="datetimerange" size="small" style="width:100%;margin-top:8px" start-placeholder="effectiveFrom" end-placeholder="effectiveTo"/>
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
              <div class="selector-box">
                <label v-for="o in ['risk-role','analyst-role','ops-role','zhang']" :key="o" class="selector-item" :class="{selected: pSubj===o}">
                  <input type="radio" :checked="pSubj===o" @change="pSubj=o"><span>{{ o }}</span>
                </label>
              </div>
              <p class="hint">主体的属性收窄(如 region)通过「行规则」中的 ${{'{subject.xxx}'}} 变量实现,不在策略层重复声明。</p>
            </template>
            <template v-else-if="wStep===2">
              <h3 class="wiz-title">资源配置</h3>
              <div class="fl">资源</div>
              <el-select v-model="pRes" size="small" style="width:100%">
                <el-option label="stream: rt_sync_c" value="rt_sync_c"/>
                <el-option label="stream: customer_wide" value="customer_wide"/>
                <el-option label="file: /data/out/**" value="/data/out/**"/>
                <el-option label="tag: 业务域/风控" value="业务域/风控"/>
              </el-select>
              <div class="fl">列白名单(不勾 = 不可见;敏感列建议配掩码)</div>
              <table class="data-table">
                <thead><tr><th style="width:40px"></th><th>列名</th><th>类型</th><th>敏感级</th></tr></thead>
                <tbody>
                  <tr v-for="row in colRows" :key="row.name">
                    <td><input type="checkbox" :checked="pCols.includes(row.name)" @change="toggleCol(row.name, $event.target.checked)"></td>
                    <td class="mono">{{ row.name }}</td>
                    <td class="dim">{{ row.type }}</td>
                    <td><span class="badge" :class="['b-l0','b-l1','b-l2','b-l2'][row.sens]">{{ 'L'+row.sens }}</span></td>
                  </tr>
                </tbody>
              </table>
              <p class="hint">继承语义:列级未显式声明时继承表级;标签子节点继承父节点;file 目录通配向下覆盖。</p>
            </template>
            <template v-else-if="wStep===3">
              <h3 class="wiz-title">动作与约束</h3>
              <div class="fl">动作(层级 + 通配;灰 = 定义层已登记、本项目未启用)</div>
              <div class="act-cat">read.*</div>
              <div class="chip-row">
                <span v-for="a in ['read.snapshot','read.changes','read.query']" :key="a" class="chip" :class="{on:pActs.includes(a)}" @click="toggleAct(a)">{{ a }}</span>
                <span class="chip off">read.aggregate</span><span class="chip off">read.sample</span>
              </div>
              <div class="act-cat">export.* / download / subscribe.*</div>
              <div class="chip-row">
                <span class="chip off">export.csv</span>
                <span class="chip" :class="{on:pActs.includes('download')}" @click="toggleAct('download')">download</span>
                <span class="chip off">subscribe.pull</span>
              </div>
              <div class="act-cat">write.*(定义层保留;本项目进侧不启用,D5)</div>
              <div class="chip-row"><span class="chip off">write.insert</span></div>

              <div class="fl" style="margin-top:12px">行谓词(内联编辑 或 引用行规则 —— 零件复用)</div>
              <el-radio-group v-model="pRowMode" size="small" style="margin-bottom:6px">
                <el-radio-button label="inline">内联编辑</el-radio-button>
                <el-radio-button label="ref">引用行规则</el-radio-button>
              </el-radio-group>
              <el-input v-if="pRowMode==='inline'" v-model="pRow" type="textarea" :rows="2" size="small" class="mono-input" @input="renderStmt"/>
              <el-select v-else v-model="pRowRef" size="small" style="width:100%">
                <el-option v-for="r in rowRuleOptions" :key="r.id" :label="`#${r.id} ${r.name}`" :value="r.id"/>
              </el-select>
              <div v-if="pRowMode==='ref'" class="hint">引用规则自带的 bind_scope 随规则生效;规则变更 → 引用它的策略版本递增(收窄 &lt;1s)。</div>
              <div v-if="pRowMode==='inline'" class="var-panel">
                <span class="var-lb">插入变量:</span>
                <span v-for="v in vars" :key="v" class="var-chip" @click="insVar(v)">${{'{'}}{{ v }}{{'}'}}</span>
              </div>
              <div class="fl">bind_scope 变量白名单(未声明 → 发布拒绝)</div>
              <div class="chip-row">
                <span v-for="b in ['subject.region','subject.username','env.channel']" :key="b" class="chip" :class="{on:pBind.includes(b)}" @click="toggleBind(b)">{{ b }}</span>
              </div>
              <div class="fl">标签范围 · 列脱敏 · 环境</div>
              <el-select v-model="pTag" size="small" style="width:100%">
                <el-option label="prefix: 业务域/风控" value="prefix:业务域/风控"/><el-option label="全部(继承)" value="all"/>
              </el-select>
              <el-select v-model="pMask" size="small" style="width:100%;margin-top:6px">
                <el-option label="phone → MASK_PHONE" value="phone=MASK_PHONE"/><el-option label="无脱敏" value=""/>
              </el-select>
              <el-select v-model="pEnvChannel" size="small" style="width:100%;margin-top:6px">
                <el-option label="channel: api" value="api"/><el-option label="channel: internal" value="internal"/><el-option label="无通道约束" value=""/>
              </el-select>
              <el-input v-model="pPurpose" size="small" placeholder="purpose 用途标注(合规审计,可空)" style="margin-top:6px"/>
              <div style="margin-top:6px"><el-switch v-model="pTwEnabled" size="small" active-text="timeWindow 时间窗"/></div>
              <el-time-picker v-if="pTwEnabled" v-model="pTwRange" is-range size="small" style="width:100%;margin-top:6px"/>
              <p class="hint">ipRange / deviceType / authLevel —— 定义层已登记,本项目未启用。</p>
            </template>
            <template v-else>
              <h3 class="wiz-title">义务与确认</h3>
              <div class="fl">义务 obligations(audit 必选)</div>
              <div class="chip-row">
                <span class="chip on">audit</span>
                <span class="chip" :class="{on:pObl.includes('rateLimit')}" @click="toggleObl('rateLimit')">rateLimit</span>
              </div>
              <div class="fl">审计级别</div>
              <el-radio-group v-model="auditLevel" size="small">
                <el-radio-button label="basic">基础</el-radio-button>
                <el-radio-button label="detail">详细</el-radio-button>
              </el-radio-group>
              <div class="fl">配额 QPS(rateLimit)</div>
              <el-input-number v-model="rateQps" size="small" :min="0" style="width:100%"/>
              <div class="fl">策略摘要</div>
              <div class="alert a-info"><span v-html="summaryHtml"/></div>
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
          <aside class="wiz-preview">
            <div class="pv-head">实时预览 <span class="badge b-off">v1</span></div>
            <el-tabs v-model="pvTab" size="small">
              <el-tab-pane label="编译产物" name="artifact">
                <div class="art"><div class="art-l">决策结果</div><div class="art-v">{{ pEffect==='deny' ? 'DENY' : 'PERMIT' }}</div></div>
                <div class="art"><div class="art-l">行级谓词(TiDB 方言)</div><div class="art-v hl">{{ artifactRow }}</div></div>
                <div class="art"><div class="art-l">绑定参数 bindOrder</div><div class="art-v">{{ artifactBinds }}</div></div>
                <div class="art"><div class="art-l">可见列</div><div class="art-v">{{ pCols.map(c=>c.split(' ')[0]).join(', ') || '—' }}</div></div>
                <div class="art"><div class="art-l">列级掩码</div><div class="art-v">{{ pMask || '无' }}</div></div>
                <div class="art"><div class="art-l">义务</div><div class="art-v">{{ pObl.join(', ') }}</div></div>
                <div class="art-l" style="margin-top:12px">编译报告(策略不声明引擎,PDP 编译预览)</div>
                <table class="data-table">
                  <thead><tr><th>引擎</th><th>下推</th><th>说明</th></tr></thead>
                  <tbody><tr v-for="r in compileReport" :key="r.engine">
                    <td>{{ r.engine }}</td>
                    <td><span class="badge" :class="r.full?'b-ok':'b-warn'">{{ r.full?'全下推':'部分' }}</span></td>
                    <td class="dim">{{ r.note }}</td>
                  </tr></tbody>
                </table>
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

      <!-- ==================== 行规则管理 ==================== -->
      <el-tab-pane label="行规则管理" name="rowrules">
        <div class="page-head"><h2 class="page-title">行规则管理</h2><span class="page-sub">cond / list / tag_ref 三类统一的「可复用零件库」,每类标注生效点</span></div>
        <div class="tri-col">
          <aside class="card">
            <h3 class="sec-title">新建行规则</h3>
            <div class="fl">资源</div>
            <el-select v-model="rrRes" size="small" style="width:100%">
              <el-option label="dataset: rt_sync_c" value="rt_sync_c"/><el-option label="dataset: dw_order" value="dw_order"/><el-option label="tag: 业务域/风控" value="tag"/>
            </el-select>
            <el-radio-group v-model="rrKind" size="small" style="margin:10px 0">
              <el-radio-button label="cond">cond 谓词</el-radio-button>
              <el-radio-button label="list">list 名单</el-radio-button>
              <el-radio-button label="tag_ref">tag_ref 标签引用</el-radio-button>
            </el-radio-group>
            <el-input v-model="rrExpr" type="textarea" :rows="2" size="small" class="mono-input"/>
            <div class="var-panel">
              <span class="var-lb">插入变量:</span>
              <span v-for="v in vars" :key="v" class="var-chip" @click="rrExpr += '${'+v+'}'">${{'{'}}{{ v }}{{'}'}}</span>
            </div>
            <div class="fl">bind_scope 白名单</div>
            <div class="chip-row"><span v-for="b in ['subject.username','subject.region']" :key="b" class="chip" :class="{on:rrBind.includes(b)}" @click="toggleRrBind(b)">{{ b }}</span></div>
            <div style="margin-top:12px"><el-button type="primary" size="small">保存规则</el-button></div>
          </aside>
          <aside class="card">
            <h3 class="sec-title">生效点(如实标注,不配不生效的策略)</h3>
            <table class="data-table">
              <thead><tr><th>出口</th><th>状态</th></tr></thead>
              <tbody><tr v-for="p in rrPoints" :key="p.outlet"><td>{{ p.outlet }}</td><td><span class="badge" :class="p.ok?'b-ok':'b-off'">{{ p.ok?'生效':'阶段二' }}</span></td></tr></tbody>
            </table>
            <div class="alert a-warn" style="margin-top:10px">tag_ref 型:标签变更触发策略版本递增,收窄 &lt;1s 生效。</div>
          </aside>
          <aside class="card">
            <h3 class="sec-title">规则列表</h3>
            <table class="data-table">
              <thead><tr><th>规则</th><th>资源</th><th>类型</th><th>变量白名单</th></tr></thead>
              <tbody><tr v-for="r in rrList" :key="r.name">
                <td><b>{{ r.name }}</b><div class="dim mono">{{ r.expr }}</div></td>
                <td>{{ r.res }}</td><td><span class="badge b-off">{{ r.kind }}</span></td><td class="dim">{{ r.bind }}</td>
              </tr></tbody>
            </table>
          </aside>
        </div>
      </el-tab-pane>

      <!-- ==================== 契约管理 ==================== -->
      <el-tab-pane label="契约管理" name="contracts">
        <div class="page-head"><h2 class="page-title">契约管理</h2><span class="page-sub">对外承诺视图:入口合一,数据仍在 contract 表;契约 ∩ 主体 = 下发视图</span></div>
        <section class="card">
          <h3 class="sec-title">契约列表</h3>
          <table class="data-table">
            <thead><tr><th>契约</th><th>消费方</th><th>覆盖流</th><th>列白名单</th><th>密级上限</th><th>行条件</th><th>通道</th><th style="width:70px"></th></tr></thead>
            <tbody><tr v-for="r in contracts" :key="r.id">
              <td class="mono">{{ r.id }}</td><td>{{ r.consumer }}</td><td>{{ r.stream }}</td><td class="mono">{{ r.cols }}</td>
              <td><span class="badge b-ok">{{ r.maxLevel }}</span></td><td class="mono">{{ r.rowCond }}</td><td><span class="badge b-off">{{ r.channel }}</span></td>
              <td><el-button link type="primary" size="small" @click="showContract(r)">预览</el-button></td>
            </tr></tbody>
          </table>
        </section>
        <section v-if="contractView" class="card">
          <h3 class="sec-title">契约视图预览(合算后 —— 下发给消费方的最终形态)</h3>
          <pre class="pv-json">{{ contractView }}</pre>
        </section>
      </el-tab-pane>

      <!-- ==================== 密级与脱敏 ==================== -->
      <el-tab-pane label="密级与脱敏" name="mask">
        <div class="page-head"><h2 class="page-title">密级与脱敏</h2><span class="page-sub">参考数据与运维页:字典为平台常量;注册表由运维登记,日常在策略编辑器选用</span></div>
        <div class="tri-col">
          <aside class="card">
            <h3 class="sec-title">敏感级字典(0-3)—— 平台常量</h3>
            <table class="data-table">
              <thead><tr><th>级</th><th>名称</th><th>含义</th></tr></thead>
              <tbody><tr v-for="d in levelDict" :key="d.lv"><td><span class="badge" :class="'b-l'+d.lv[1]">{{ d.lv }}</span></td><td>{{ d.name }}</td><td class="dim">{{ d.desc }}</td></tr></tbody>
            </table>
          </aside>
          <aside class="card">
            <h3 class="sec-title">脱敏规则注册表(mask_rule;两端同源,版本不一致拒下发)</h3>
            <table class="data-table">
              <thead><tr><th>编码</th><th>参数</th><th>实现版本</th><th>平台侧</th><th>计算面</th></tr></thead>
              <tbody><tr v-for="r in maskRules" :key="r.code">
                <td class="mono">{{ r.code }}</td><td>{{ r.param }}</td><td>{{ r.ver }}</td>
                <td><span class="badge b-ok">{{ r.ver }}</span></td>
                <td><span class="badge" :class="r.sync?'b-ok':'b-warn'">{{ r.sync?'已同步':'待同步' }}</span></td>
              </tr></tbody>
            </table>
            <div style="margin-top:12px"><el-button type="primary" size="small" @click="maskDialog=true">+ 注册新规则</el-button></div>
          </aside>
        </div>
        <el-dialog v-model="maskDialog" title="注册脱敏规则(运维操作)" width="420px">
          <el-form label-width="90px" size="small">
            <el-form-item label="规则编码"><el-input v-model="newMask.code" placeholder="如 MASK_EMAIL"/></el-form-item>
            <el-form-item label="参数"><el-input v-model="newMask.param" placeholder="如 保留前2后4"/></el-form-item>
            <el-form-item label="实现版本"><el-input v-model="newMask.ver" placeholder="v1"/></el-form-item>
          </el-form>
          <div class="hint">注册后需在平台侧与计算面同步实现;两端版本不一致时,含该规则的策略下发会被拒(fail-closed)。</div>
          <template #footer>
            <el-button size="small" @click="maskDialog=false">取消</el-button>
            <el-button type="primary" size="small" @click="registerMask">注册</el-button>
          </template>
        </el-dialog>
      </el-tab-pane>

      <!-- ==================== 有效权限预览 ==================== -->
      <el-tab-pane label="有效权限预览" name="preview">
        <div class="page-head"><h2 class="page-title">有效权限预览</h2><span class="page-sub">看见最终结果:某主体到底能看什么、为什么</span></div>
        <div class="tri-col">
          <aside class="card">
            <h3 class="sec-title">查询条件</h3>
            <el-select v-model="pvSubj" size="small" style="width:100%;margin-bottom:8px" @change="renderPreview">
              <el-option label="risk-role(角色)" value="risk-role"/><el-option label="zhang(用户)" value="zhang"/>
            </el-select>
            <el-select v-model="pvRes" size="small" style="width:100%;margin-bottom:8px" @change="renderPreview">
              <el-option label="stream: rt_sync_c" value="rt_sync_c"/><el-option label="stream: dw_order" value="dw_order"/>
            </el-select>
            <el-select v-model="pvAct" size="small" style="width:100%" @change="renderPreview">
              <el-option label="read.snapshot" value="snapshot"/><el-option label="read.changes" value="changes"/>
            </el-select>
          </aside>
          <aside class="card">
            <h3 class="sec-title">最终结果(判定合成)</h3>
            <table class="data-table">
              <thead><tr><th>项</th><th>结果</th></tr></thead>
              <tbody><tr v-for="r in pvResult" :key="r.k"><td>{{ r.k }}</td><td v-html="r.v"/></tr></tbody>
            </table>
          </aside>
          <aside class="card">
            <h3 class="sec-title">为什么 —— 判定链解释</h3>
            <div v-for="(w,i) in pvWhy" :key="i" class="why"><div class="why-t">{{ w.t }}</div><div class="why-d">{{ w.d }}</div></div>
          </aside>
        </div>
      </el-tab-pane>

      <!-- ==================== 变更审计 ==================== -->
      <el-tab-pane label="变更审计" name="audit">
        <div class="page-head"><h2 class="page-title">变更审计</h2><span class="page-sub">谁在何时把什么改成了什么(策略变更历史)</span></div>
        <section class="card">
          <table class="data-table">
            <thead><tr><th>时间</th><th>操作人</th><th>动作</th><th>对象</th><th>变更前 → 变更后</th></tr></thead>
            <tbody><tr v-for="a in audits" :key="a.time">
              <td class="dim">{{ a.time }}</td><td>{{ a.who }}</td>
              <td><span class="badge" :class="a.act==='删除'?'b-deny':(a.act==='拒绝'?'b-deny':'b-ok')">{{ a.act }}</span></td>
              <td class="mono">{{ a.obj }}</td><td class="dim">{{ a.delta }}</td>
            </tr></tbody>
          </table>
        </section>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useRoute, useRouter } from 'vue-router'
// TODO: 接入后端 API —— 本页全部为 mock 数据,接口契约见《数据权限统一控制模型设计》§8
const route = useRoute()
const router = useRouter()
const tab = ref('overview')
const TAB_NAMES = ['overview', 'resource', 'editor', 'rowrules', 'contracts', 'mask', 'preview', 'audit']
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
  { kind: '资源级(列/密级)', '平台 REST': true, 'v2 计算面': true, 'Spark 批': true, 'Flink 流': true, '订阅 PULL': true, '文件下载': true },
  { kind: '行规则 cond', '平台 REST': true, 'v2 计算面': true, 'Spark 批': true, 'Flink 流': true, '订阅 PULL': true, '文件下载': false },
  { kind: '行规则 tag_ref', '平台 REST': true, 'v2 计算面': false, 'Spark 批': false, 'Flink 流': false, '订阅 PULL': true, '文件下载': false },
]

/* ---------- 资源授权 ---------- */
const subjType = ref('role')
const subject = ref('risk-role')
const subjectOptions = computed(() => subjType.value === 'role' ? ['risk-role', 'analyst-role', 'ops-role'] : ['zhang', 'li'])
const resTree = [
  { key: 'rt_sync_c', label: '📊 rt_sync_c', children: [
    { key: 'rt_sync_c.id', label: 'id (key)' }, { key: 'rt_sync_c.v', label: 'v' },
    { key: 'rt_sync_c.seq', label: 'seq' }, { key: 'rt_sync_c.phone', label: 'phone' } ] },
  { key: 'customer_wide', label: '📊 customer_wide', children: [ { key: 'customer_wide.name', label: 'name' } ] },
  { key: '/data/out/**', label: '📁 /data/out/**' },
  { key: '业务域/风控/黑名单', label: '🏷️ 业务域/风控/黑名单' },
]
const treeProps = { label: 'label' }
const curRes = ref('rt_sync_c')
const level = ref('L3')
const effect = ref('grant')
const levels = [ { k: 'L0', v: '不可见' }, { k: 'L1', v: '元数据' }, { k: 'L2', v: '脱敏' }, { k: 'L3', v: '全量' } ]
const onResClick = n => { if (!n.children) curRes.value = n.key }
const policiesMap = {
  'risk-role': [ { res: 'stream: rt_sync_c', lv: 'L2', eff: 'grant' }, { res: 'stream: dw_order', lv: 'L3', eff: 'grant' }, { res: 'tag: 密级/机密', lv: 'L0', eff: 'deny' } ],
  'analyst-role': [ { res: 'stream: customer_wide', lv: 'L2', eff: 'grant' } ],
  'ops-role': [ { res: 'file: /data/out/**', lv: 'L3', eff: 'grant' } ],
  zhang: [ { res: 'stream: rt_sync_c', lv: 'L3', eff: 'grant' } ],
  li: [],
}
const curPolicies = computed(() => policiesMap[subject.value] || [])
const saveRes = () => ElMessage.success('已保存(原型):策略写库 → 版本号 +1 → 失效事件广播')

/* ---------- 策略编辑器(五步向导) ---------- */
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
const toggleAct = a => { const i = pActs.value.indexOf(a); i >= 0 ? pActs.value.splice(i, 1) : pActs.value.push(a) }
const pCols = ref(['id (key)', 'v', 'seq'])
const colRows = [
  { name: 'id (key)', type: 'bigint', sens: 0 },
  { name: 'v', type: 'varchar', sens: 0 },
  { name: 'seq', type: 'bigint', sens: 1 },
  { name: 'phone', type: 'varchar', sens: 2 },
  { name: 'region', type: 'varchar', sens: 1 },
  { name: 'status', type: 'varchar', sens: 0 },
]
const toggleCol = (name, v) => { const i = pCols.value.indexOf(name); if (v && i < 0) pCols.value.push(name); else if (!v && i >= 0) pCols.value.splice(i, 1) }
const pRowMode = ref('inline')
const pRowRef = ref(24)
const rowRuleOptions = [
  { id: 24, name: '仅本人订单(owner = ${subject.username})' },
  { id: 17, name: '华东白名单(region IN 华东,华北)' },
  { id: 9, name: '风控可见域(tag IN 业务域/风控/**)' },
]
const pRow = ref("status = 'paid' AND region = ${subject.region}")
const pBind = ref(['subject.region'])
const toggleBind = b => { const i = pBind.value.indexOf(b); i >= 0 ? pBind.value.splice(i, 1) : pBind.value.push(b) }
const pTag = ref('prefix:业务域/风控')
const pMask = ref('phone=MASK_PHONE')
const pEnvChannel = ref('api')
const pPurpose = ref('')
const pTwEnabled = ref(false)
const pTwRange = ref(null)
const pObl = ref(['audit'])
const toggleObl = o => { const i = pObl.value.indexOf(o); i >= 0 ? pObl.value.splice(i, 1) : pObl.value.push(o) }
const auditLevel = ref('detail')
const rateQps = ref(100)
const pDraft = ref(false)
const pWindow = ref(null)
const vars = ['subject.username', 'subject.region', 'env.channel', 'env.time']
const insVar = v => { pRow.value += '${' + v + '}' }
const stmtErr = ref('')
const renderStmt = () => {
  const used = [...pRow.value.matchAll(/\$\{([^}]+)\}/g)].map(m => m[1])
  const miss = pRowMode.value === 'inline' ? used.filter(v => !pBind.value.includes(v)) : []
  stmtErr.value = miss.length ? '校验失败:谓词变量 ' + miss.join(', ') + ' 未在 bind_scope 声明 —— 策略拒绝发布(fail-closed)。' : ''
}
const artifactRow = computed(() => {
  const src = pRowMode.value === 'ref' ? rowRuleOptions.find(r => r.id === pRowRef.value)?.name || '' : pRow.value
  return src.replace(/\$\{([^}]+)\}/g, '?') || '—'
})
const artifactBinds = computed(() => {
  const src = pRowMode.value === 'ref' ? '' : pRow.value
  const binds = [...src.matchAll(/\$\{([^}]+)\}/g)].map((m, i) => `[${i}] ${m[1]}`)
  return binds.join('\n') || '无'
})
const prettyJson = computed(() => {
  const rowPart = pRowMode.value === 'ref' ? { mode: 'ref', ruleRef: 'rr-' + pRowRef.value, expr: null } : { mode: 'inline', ruleRef: null, expr: pRow.value }
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
const compileReport = [
  { engine: 'TiDB (SQL)', full: true, note: '列投影 + WHERE 参数化全下推' },
  { engine: 'Trino/Iceberg', full: true, note: 'SQL 重写全下推' },
  { engine: 'ES (DSL)', full: false, note: 'rowExpr 含会话变量 → 退残留段' },
  { engine: 'Spark 执行体', full: true, note: 'DataFrame filter(Plan 注入)' },
  { engine: 'Flink 执行体', full: true, note: '进程内求值(Plan 注入)' },
]
const saveDraft = () => ElMessage.success('草稿已保存(原型):draft 状态不参与判定')
const simulate = () => { renderStmt(); if (stmtErr.value) { ElMessage.error('测试仿真被拒:' + stmtErr.value); return } ElMessage.success('测试仿真(原型):PERMIT · 行级谓词已渲染 · 敏感列已掩码 · 见右侧编译产物') }
const wizardPublish = () => { renderStmt(); if (stmtErr.value) { ElMessage.error('发布被拒(校验清单):' + stmtErr.value); return } ElMessage.success('已发布(原型):策略写库 → 版本号+1 → 失效事件广播 → 各 PEP 本地缓存失效') }

/* ---------- 行规则 ---------- */
const rrRes = ref('rt_sync_c')
const rrKind = ref('cond')
const rrExpr = ref('owner = ${subject.username}')
const rrBind = ref(['subject.username'])
const toggleRrBind = b => { const i = rrBind.value.indexOf(b); i >= 0 ? rrBind.value.splice(i, 1) : rrBind.value.push(b) }
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
const maskDialog = ref(false)
const newMask = ref({ code: '', param: '', ver: 'v1' })
const registerMask = () => {
  if (!newMask.value.code) { ElMessage.warning('请填写规则编码'); return }
  maskRules.push({ code: newMask.value.code, param: newMask.value.param || '—', ver: newMask.value.ver, sync: false })
  maskDialog.value = false
  ElMessage.success('已注册(原型):两端同步实现后可被策略选用;未同步前含该规则的策略拒下发')
}

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
      { k: '最终等级', v: '<span class="badge b-l3">L3 全量</span>' },
      { k: '可见列', v: 'id, v, seq, phone' },
      { k: '行规则', v: '无' }, { k: '脱敏', v: '无' }, { k: '标签', v: '全部(继承)' },
    ]
    pvWhy.value = [
      { t: '① 契约', d: 'ct-tag-rt_sync_c 覆盖该流,grant 全部列(通过)' },
      { t: '② 主体显式(user)', d: '张工 对该流有 user 级 L3 授权 —— 覆盖 role 级' },
      { t: '③ scope 前缀', d: '无更细限制(通过)' },
      { t: '④ 敏感级兜底', d: 'phone 敏感级 2 ≤ L3(通过)' },
      { t: '⑤ 结论', d: 'L3 全量。phone 敏感级 2,若主体降级到 L2 将触发脱敏。' },
    ]
  } else {
    pvResult.value = [
      { k: '最终等级', v: '<span class="badge b-l2">L2 脱敏</span>' },
      { k: '可见列', v: 'id, v, seq' },
      { k: '行规则', v: "status='paid' AND region=${subject.region}" },
      { k: '脱敏', v: 'phone → MASK_PHONE' }, { k: '标签', v: 'prefix: 业务域/风控' },
    ]
    pvWhy.value = [
      { t: '① 契约', d: 'ct-sub-rt_sync_c:列白名单 [id,v] ∩ 密级上限 2(收窄)' },
      { t: '② 主体 role', d: 'risk-role 对该流 L2 —— 列交集 = 契约∩主体 = [id,v,seq]' },
      { t: '③ scope 前缀', d: '标签“业务域/风控”前缀继承 L2(收窄)' },
      { t: '④ 行规则', d: '契约行条件 + 主体行规则 —— AND 合并' },
      { t: '⑤ 脱敏', d: 'phone 敏感级 2 → MASK_PHONE' },
      { t: '⑥ 结论', d: 'L2 脱敏可见;越权列连 key 都不出现。' },
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
/* ===== 设计语言(参考「权限系统设计」) ===== */
:root { --primary: #2563eb; }
.perm-center { padding: 14px 18px; }
.page-head { display: flex; align-items: baseline; gap: 10px; margin-bottom: 14px; }
.page-title { font-size: 17px; font-weight: 650; margin: 0; }
.page-sub { color: #94a3b8; font-size: 12.5px; }
.card { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 18px 20px; margin-bottom: 14px; }
.sec-title { font-size: 14px; font-weight: 650; margin: 0 0 14px; display: flex; align-items: center; gap: 8px; }
.sec-title::before { content: ''; width: 3px; height: 15px; background: var(--primary); border-radius: 2px; }
.stat-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 14px; }
.stat-card { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 14px; }
.stat-n { font-size: 24px; font-weight: 700; }
.stat-l { color: #64748b; font-size: 12px; margin-top: 2px; }
.badge { display: inline-block; padding: 2px 9px; border-radius: 99px; font-size: 11px; font-weight: 600; }
.b-ok { background: #dcfce7; color: #15803d; }
.b-deny { background: #fee2e2; color: #b91c1c; }
.b-warn { background: #fef9c3; color: #a16207; }
.b-off { background: #f1f5f9; color: #94a3b8; }
.b-l0 { background: #fee2e2; color: #b91c1c; }
.b-l1 { background: #ffedd5; color: #c2410c; }
.b-l2 { background: #fef9c3; color: #a16207; }
.b-l3 { background: #dcfce7; color: #15803d; }
.data-table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
.data-table th { text-align: left; padding: 8px 10px; background: #f8fafc; border-bottom: 1px solid #e2e8f0; font-weight: 600; color: #64748b; font-size: 11.5px; }
.data-table td { padding: 8px 10px; border-bottom: 1px solid #f1f5f9; }
.data-table tr:last-child td { border-bottom: none; }
.mono { font-family: ui-monospace, Menlo, monospace; font-size: 12px; }
.dim { color: #94a3b8; font-size: 12px; }
.hint { font-size: 11.5px; color: #94a3b8; margin-top: 8px; line-height: 1.6; }
.fl { color: #64748b; font-size: 12px; margin: 10px 0 4px; }
.tri-col { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; align-items: start; }
.selector-box { border: 1px solid #e2e8f0; border-radius: 8px; max-height: 200px; overflow-y: auto; background: #fff; }
.selector-item { display: flex; align-items: center; gap: 8px; padding: 8px 12px; cursor: pointer; border-bottom: 1px solid #f1f5f9; font-size: 13px; }
.selector-item:last-child { border-bottom: none; }
.selector-item:hover { background: #f8fafc; }
.selector-item.selected { background: #eff6ff; color: #2563eb; font-weight: 500; }
.lv-row { display: flex; gap: 6px; }
.lv { flex: 1; text-align: center; padding: 8px 2px; border: 1px solid #e2e8f0; border-radius: 8px; cursor: pointer; font-size: 12px; background: #fff; }
.lv.on { outline: 2px solid var(--primary); }
.lv-sub { font-size: 10px; color: #94a3b8; }
.alert { padding: 10px 12px; border-radius: 8px; font-size: 12px; line-height: 1.6; }
.a-err { background: #fef2f2; border: 1px solid #fecaca; color: #991b1b; }
.a-info { background: #eff6ff; border: 1px solid #bfdbfe; color: #1d4ed8; }
.a-warn { background: #fffbeb; border: 1px solid #fde68a; color: #92400e; }
.chip-row { display: flex; flex-wrap: wrap; gap: 8px; margin: 6px 0; }
.chip { display: inline-block; padding: 3px 11px; border: 1px solid #e2e8f0; border-radius: 99px; font-size: 12px; cursor: pointer; background: #fff; }
.chip:hover { border-color: var(--primary); color: var(--primary); }
.chip.on { background: var(--primary); border-color: var(--primary); color: #fff; }
.chip.off { color: #cbd5e1; border-style: dashed; cursor: not-allowed; }
.act-cat { font-size: 11px; font-weight: 700; color: #94a3b8; text-transform: uppercase; letter-spacing: .5px; margin: 8px 0 2px; }
.var-panel { display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 10px; background: #fffbeb; border: 1px solid #fde68a; border-radius: 8px; margin: 6px 0; align-items: center; }
.var-lb { font-size: 11px; color: #92400e; }
.var-chip { padding: 2px 8px; background: #fff; border: 1px solid #fde68a; border-radius: 4px; color: #92400e; cursor: pointer; font-family: ui-monospace, Menlo, monospace; font-size: 11px; }
.var-chip:hover { border-color: #d97706; }
.mono-input :deep(textarea) { font-family: ui-monospace, Menlo, monospace; }
.tl-item { display: flex; align-items: center; gap: 10px; padding: 8px 0; border-bottom: 1px solid #f1f5f9; font-size: 12.5px; }
.tl-item:last-child { border-bottom: none; }
.tl-main { flex: 1; min-width: 0; }
.tl-sub { color: #94a3b8; margin-left: 8px; }
.tl-time { color: #94a3b8; font-size: 11.5px; }
.why { border-left: 3px solid #16a34a; padding: 8px 12px; margin: 8px 0; background: #f0fdf4; border-radius: 0 8px 8px 0; }
.why-t { font-weight: 600; font-size: 12.5px; }
.why-d { font-size: 12px; color: #64748b; margin-top: 2px; }
/* ===== 向导式策略编辑器 ===== */
.wizard { display: grid; grid-template-columns: 190px 1fr 330px; gap: 12px; min-height: 520px; }
.wiz-nav { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 14px 0; }
.wiz-step { display: flex; gap: 10px; padding: 12px 16px; cursor: pointer; }
.wiz-step:hover { background: #f8fafc; }
.wiz-step.active { background: #eff6ff; }
.wiz-idx { width: 22px; height: 22px; border-radius: 50%; border: 2px solid #cbd5e1; display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 600; color: #64748b; flex-shrink: 0; }
.wiz-step.active .wiz-idx { background: var(--primary); border-color: var(--primary); color: #fff; }
.wiz-step.done .wiz-idx { background: #16a34a; border-color: #16a34a; color: #fff; }
.wiz-name { font-size: 13px; font-weight: 500; color: #1e293b; }
.wiz-step.active .wiz-name { color: var(--primary); }
.wiz-desc { font-size: 11px; color: #94a3b8; margin-top: 2px; }
.wiz-form { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 20px 22px; display: flex; flex-direction: column; }
.wiz-title { font-size: 16px; font-weight: 650; margin: 0 0 14px; }
.wiz-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.wiz-col { display: flex; flex-direction: column; }
.wiz-actions { display: flex; align-items: center; gap: 8px; margin-top: auto; padding-top: 16px; border-top: 1px solid #e2e8f0; }
.wiz-preview { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px 12px; overflow: auto; }
.pv-head { display: flex; justify-content: space-between; align-items: center; font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.art { margin-bottom: 8px; }
.art-l { font-size: 10.5px; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: .4px; margin-bottom: 3px; }
.art-v { font-family: ui-monospace, Menlo, monospace; font-size: 11.5px; color: #1e293b; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 6px 8px; word-break: break-all; white-space: pre-wrap; }
.art-v.hl { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
.pv-json { font-family: ui-monospace, Menlo, monospace; font-size: 11px; background: #0f172a; color: #e2e8f0; border-radius: 8px; padding: 10px 12px; line-height: 1.6; white-space: pre-wrap; margin: 0; max-height: 460px; overflow: auto; }
.ck { display: flex; gap: 8px; padding: 7px 0; border-bottom: 1px solid #f1f5f9; font-size: 12.5px; }
.ck:last-child { border-bottom: none; }
.ck-i { width: 16px; height: 16px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 10px; flex-shrink: 0; margin-top: 2px; }
.ck.ok .ck-i { background: #f0fdf4; color: #16a34a; }
.ck.warn .ck-i { background: #fffbeb; color: #d97706; }
.ck.err .ck-i { background: #fef2f2; color: #dc2626; }
.ck-d { font-size: 11px; color: #94a3b8; margin-top: 1px; word-break: break-all; }
</style>

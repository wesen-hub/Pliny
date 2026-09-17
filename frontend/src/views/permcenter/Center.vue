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

      <!-- ==================== 策略编辑器(完全版) ==================== -->
      <el-tab-pane label="策略编辑器" name="editor">
        <el-row :gutter="12">
          <el-col :span="8">
            <el-card shadow="never">
              <template #header>① 效果与主体</template>
              <el-radio-group v-model="pEffect" size="small">
                <el-radio-button label="grant">grant 授予</el-radio-button>
                <el-radio-button label="deny">deny 拒绝</el-radio-button>
              </el-radio-group>
              <div class="fl">主体类型(框架支持 dept,本项目暂未启用)</div>
              <el-select v-model="pSubjType" size="small" style="width:100%">
                <el-option label="角色 role" value="role"/>
                <el-option label="用户 user" value="user"/>
                <el-option label="部门 dept(未启用)" value="dept" disabled/>
              </el-select>
              <div class="fl">主体</div>
              <el-select v-model="pSubj" size="small" style="width:100%">
                <el-option v-for="o in ['risk-role','analyst-role','ops-role','zhang']" :key="o" :label="o" :value="o"/>
              </el-select>
              <div class="fl">显式优先级 priority(同 specificity 冲突时用;缺省 0,大者优先)</div>
              <el-input-number v-model="pPriority" size="small" :min="0" :max="99" style="width:100%"/>
              <el-alert type="info" :closable="false" show-icon style="margin-top:8px"
                title="冲突消解三级:① specificity 链(deny 短路 → user > role > scope > 兜底)② 显式 priority ③ 仍歧义 → 发布期拒绝"/>
            </el-card>
          </el-col>
          <el-col :span="8">
            <el-card shadow="never">
              <template #header>② 资源与动作</template>
              <div class="fl">资源</div>
              <el-select v-model="pRes" size="small" style="width:100%">
                <el-option label="stream: rt_sync_c" value="rt_sync_c"/>
                <el-option label="stream: customer_wide" value="customer_wide"/>
                <el-option label="file: /data/out/**" value="/data/out/**"/>
              </el-select>
              <div class="fl">动作(层级 + 通配;灰色 = 定义层已登记、本项目未启用)</div>
              <div style="font-size:12px;margin:4px 0 2px"><b>read.*</b> <el-checkbox size="small" :model-value="pActs.includes('read.snapshot')&&pActs.includes('read.changes')" @change="v=>toggleReadAll(v)">全选</el-checkbox></div>
              <el-checkbox-group v-model="pActs" size="small">
                <el-checkbox label="read.snapshot"/>
                <el-checkbox label="read.changes"/>
                <el-checkbox label="read.query"/>
                <el-checkbox label="read.aggregate" disabled/>
                <el-checkbox label="read.sample" disabled/>
              </el-checkbox-group>
              <div style="font-size:12px;margin:8px 0 2px"><b>export.* / download / subscribe.*</b></div>
              <el-checkbox-group v-model="pActs" size="small">
                <el-checkbox label="export.csv" disabled/>
                <el-checkbox label="export.json" disabled/>
                <el-checkbox label="export.file" disabled/>
                <el-checkbox label="download"/>
                <el-checkbox label="subscribe.pull" disabled/>
                <el-checkbox label="subscribe.push" disabled/>
              </el-checkbox-group>
              <div style="font-size:12px;margin:8px 0 2px"><b>write.*</b>(定义层保留;本项目进侧不启用,D5)</div>
              <el-checkbox-group size="small">
                <el-checkbox label="write.insert" disabled/>
                <el-checkbox label="write.update" disabled/>
                <el-checkbox label="write.delete" disabled/>
              </el-checkbox-group>
              <div class="fl">列白名单(不勾 = 不可见;key 列单独标记)</div>
              <el-checkbox-group v-model="pCols" size="small">
                <el-checkbox label="id (key)"/>
                <el-checkbox label="v"/>
                <el-checkbox label="seq"/>
                <el-checkbox label="phone"/>
                <el-checkbox label="_commit_ts"/>
              </el-checkbox-group>
            </el-card>
          </el-col>
          <el-col :span="8">
            <el-card shadow="never">
              <template #header>③ 约束(行/标签/密级/环境)</template>
              <div class="fl">行谓词(内联编辑 或 引用已定义的行规则——零件复用)</div>
              <el-radio-group v-model="pRowMode" size="small" style="margin-bottom:6px">
                <el-radio-button label="inline">内联编辑</el-radio-button>
                <el-radio-button label="ref">引用行规则</el-radio-button>
              </el-radio-group>
              <el-input v-if="pRowMode==='inline'" v-model="pRow" type="textarea" :rows="2" size="small" @input="renderStmt"/>
              <el-select v-else v-model="pRowRef" size="small" style="width:100%" @change="renderStmt">
                <el-option v-for="r in rowRuleOptions" :key="r.id" :label="`#${r.id} ${r.name}`" :value="r.id"/>
              </el-select>
              <div v-if="pRowMode==='ref'" class="hint2">引用规则自带的 bind_scope 白名单将一并生效;规则变更会递增策略版本(收窄 &lt;1s)。</div>
              <div class="chips" v-if="pRowMode==='inline'">
                <span v-for="v in vars" :key="v" class="chip" @click="insVar(v)">${{'{'}}{{ v }}{{'}'}}</span>
              </div>
              <div class="fl">bind_scope 变量白名单(谓词用到的必须声明)</div>
              <el-checkbox-group v-model="pBind" size="small" :disabled="pRowMode==='ref'">
                <el-checkbox label="subject.region"/>
                <el-checkbox label="subject.username"/>
                <el-checkbox label="env.channel"/>
              </el-checkbox-group>
              <div class="fl">标签范围</div>
              <el-select v-model="pTag" size="small" style="width:100%">
                <el-option label="prefix: 业务域/风控" value="prefix:业务域/风控"/>
                <el-option label="全部(继承)" value="all"/>
              </el-select>
              <div class="fl">列脱敏(义务 mask)</div>
              <el-select v-model="pMask" size="small" style="width:100%">
                <el-option label="phone → MASK_PHONE" value="phone=MASK_PHONE"/>
                <el-option label="无脱敏" value=""/>
              </el-select>
              <div class="fl">环境约束(定义层全集;本项目启用前三项)</div>
              <el-select v-model="pEnvChannel" size="small" style="width:100%">
                <el-option label="channel: api" value="api"/>
                <el-option label="channel: internal" value="internal"/>
                <el-option label="无通道约束" value=""/>
              </el-select>
              <el-input v-model="pPurpose" size="small" placeholder="purpose 用途标注(合规审计,可空)" style="margin-top:6px"/>
              <div style="margin-top:6px">
                <el-switch v-model="pTwEnabled" size="small" active-text="timeWindow 时间窗"/>
              </div>
              <el-time-picker v-if="pTwEnabled" v-model="pTwRange" is-range size="small" style="width:100%;margin-top:6px" start-placeholder="开始" end-placeholder="结束"/>
              <div class="hint2" style="margin-top:6px">ipRange / deviceType / authLevel —— 定义层已登记,本项目未启用。</div>
              <div class="fl">义务 obligations(默认必选 audit)</div>
              <el-checkbox-group v-model="pObl" size="small">
                <el-checkbox label="audit" disabled checked/>
                <el-checkbox label="rateLimit"/>
              </el-checkbox-group>
            </el-card>
          </el-col>
        </el-row>
        <el-row :gutter="12" style="margin-top:12px">
          <el-col :span="12">
            <el-card shadow="never" header="④ 生命周期与发布">
              <el-switch v-model="pDraft" active-text="draft(草稿,不生效)" inactive-text="active(立即生效)"/>
              <div style="margin-top:10px;color:#64748b;font-size:12px">定时生效(框架能力):</div>
              <el-date-picker v-model="pWindow" type="datetimerange" size="small" style="width:100%" start-placeholder="effectiveFrom" end-placeholder="effectiveTo"/>
              <div style="margin-top:12px">
                <el-button type="primary" size="small" @click="publish">发布(校验 + 版本号递增)</el-button>
              </div>
              <el-alert v-if="stmtErr" type="error" :closable="false" show-icon :title="stmtErr" style="margin-top:10px"/>
              <el-alert v-else type="success" :closable="false" show-icon title="校验通过:变量已声明;发布后收窄 <1s 生效。" style="margin-top:10px"/>
            </el-card>
          </el-col>
          <el-col :span="12">
            <el-card shadow="never" header="策略语句预览(引擎无关的抽象语义 —— 存入策略库的内容)">
              <pre class="stmt">{{ stmt }}</pre>
            </el-card>
            <el-card shadow="never" header="可下推性编译报告(发布预览;策略不声明引擎,本报告为 PDP 编译预览)" style="margin-top:12px">
              <el-table :data="compileReport" size="small" border>
                <el-table-column prop="engine" label="引擎" width="130"/>
                <el-table-column prop="level" label="下推程度" width="110"><template #default="{row}"><el-tag :type="row.full?'success':'warning'" size="small">{{ row.full?'全下推':'部分下推' }}</el-tag></template></el-table-column>
                <el-table-column prop="note" label="说明"/>
              </el-table>
              <div class="hint2" style="margin-top:6px">“部分下推”= 未下推成分自动降级为残留段(结果层过滤),正确性不变,性能可观测;执行层适配器可裁剪(见设计 §3.5)。</div>
            </el-card>
          </el-col>
        </el-row>
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

/* ---------- 策略编辑器 ---------- */
const pEffect = ref('grant')
const pSubjType = ref('role')
const pSubj = ref('risk-role')
const pPriority = ref(0)
const pRes = ref('rt_sync_c')
const pActs = ref(['read.snapshot', 'read.changes'])
const toggleReadAll = v => {
  const base = ['read.snapshot', 'read.changes']
  pActs.value = v ? [...new Set([...pActs.value, ...base])] : pActs.value.filter(a => !base.includes(a))
}
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
const pDraft = ref(false)
const pWindow = ref(null)
const vars = ['subject.username', 'subject.region', 'env.channel', 'env.time']
const insVar = v => { pRow.value += '${' + v + '}' }
const stmtErr = ref('')
const stmt = ref('')
const compileReport = [
  { engine: 'TiDB (SQL)', full: true, note: '列投影 + WHERE 参数化全下推' },
  { engine: 'Trino/Iceberg', full: true, note: 'SQL 重写全下推' },
  { engine: 'ES (DSL)', full: false, note: 'rowExpr 含 ${subject.region} → 退残留段(结果层过滤)' },
  { engine: 'Spark 执行体', full: true, note: 'DataFrame filter + 列投影(Plan 注入)' },
  { engine: 'Flink 执行体', full: true, note: '进程内求值(Plan 注入)' },
]
const renderStmt = () => {
  const used = [...pRow.value.matchAll(/\$\{([^}]+)\}/g)].map(m => m[1])
  const miss = pRowMode.value === 'inline' ? used.filter(v => !pBind.value.includes(v)) : []
  stmtErr.value = miss.length ? '校验失败:谓词变量 ' + miss.join(', ') + ' 未在 bind_scope 声明 —— 策略拒绝发布(fail-closed)。' : ''
  const rowPart = pRowMode.value === 'ref'
    ? `ref:#${pRowRef.value} ${rowRuleOptions.find(r => r.id === pRowRef.value)?.name || ''}`
    : (pRow.value || '(无)')
  const envParts = []
  if (pEnvChannel.value) envParts.push('channel=' + pEnvChannel.value)
  if (pPurpose.value) envParts.push('purpose=' + pPurpose.value)
  if (pTwEnabled.value) envParts.push('timeWindow=启用')
  stmt.value = `effect:   ${pEffect.value}
subject:  ${pSubj.value}
resource: ${pRes.value}
actions:  ${pActs.value.join(', ')}
columns:  ${pCols.value.map(c => c.split(' ')[0]).join(', ')}
rowExpr:  ${rowPart}
mask:     ${pMask.value || '(无)'}
tagScope: ${pTag.value}
env:      ${envParts.join(' · ') || '(无)'}
obligations: ${pObl.value.join(', ')}
priority: ${pPriority.value}
lifecycle: ${pDraft.value ? 'draft' : 'active'}`
}
renderStmt()
const publish = () => {
  renderStmt()
  if (stmtErr.value) { ElMessage.error('发布被拒:变量未声明'); return }
  ElMessage.success('已发布(原型):策略写库 → 版本号 +1 → 收窄事件广播 → 各出口 PEP 失效旧 Plan')
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
:deep(.tag-l2) { background: #fef9c3; color: #a16207; padding: 2px 8px; border-radius: 99px; font-size: 11px; font-weight: 600; }
:deep(.tag-l3) { background: #dcfce7; color: #15803d; padding: 2px 8px; border-radius: 99px; font-size: 11px; font-weight: 600; }
</style>

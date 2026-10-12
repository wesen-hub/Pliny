<template>
  <div class="page">
    <div class="page-header">
      <h2>全链路可视化</h2>
      <div class="sub">P0~P7 八阶段 38 步（与设计文档 §20.2 一一对应）；选择路径后逐步播放，观察请求在五条泳道间的流转</div>
    </div>

    <!-- 控制区 -->
    <div class="card">
      <div class="controls">
        <el-radio-group v-model="pathKey" size="small" @change="resetPlay">
          <el-radio-button value="cold">冷链路（38 步全流程）</el-radio-button>
          <el-radio-button value="fast">绿色快速通道（L1 命中）</el-radio-button>
        </el-radio-group>
        <el-divider direction="vertical" />
        <el-button size="small" @click="stepBack" :disabled="curIdx <= 0">← 上一步</el-button>
        <el-button size="small" type="primary" @click="stepNext" :disabled="curIdx >= seq.length - 1">
          下一步 →
        </el-button>
        <el-button size="small" :type="playing ? 'danger' : 'success'" @click="togglePlay">
          {{ playing ? '⏸ 暂停' : '▶ 自动播放' }}
        </el-button>
        <el-button size="small" text @click="resetPlay">重置</el-button>
        <div class="progress">
          进度：<b>{{ Math.min(curIdx + 1, seq.length) }}</b> / {{ seq.length }} 步
          <el-progress :percentage="Math.round(((curIdx + 1) / seq.length) * 100)" :show-text="false" style="width: 140px; display: inline-block; margin-left: 8px" />
        </div>
      </div>
      <div class="path-desc">{{ curPath.desc }}</div>
      <div class="legend">
        <span v-for="(t, key) in typeMeta" :key="key" class="legend-item">
          <i :style="{ background: t.color }"></i>{{ t.label }}
        </span>
      </div>
    </div>

    <!-- 泳道 -->
    <div class="lanes">
      <div v-for="lane in lanes" :key="lane.id" class="lane">
        <div class="lane-head" :style="{ borderColor: lane.color }">
          <div class="lane-title" :style="{ color: lane.color }">{{ lane.label }}</div>
          <div class="lane-short">{{ lane.short }}</div>
        </div>
        <div class="lane-body">
          <div class="lane-phase" v-for="ph in lanePhases(lane.id)" :key="ph.id">
            <div class="phase-tag">{{ ph.id }}</div>
            <div class="phase-steps">
              <div
                v-for="s in laneSteps(lane.id, ph.id)"
                :key="s.no"
                class="step-card"
                :class="stepState(s)"
                :style="stepStyle(s)"
                @click="jumpTo(s)"
              >
                <div class="step-no">{{ s.icon }}</div>
                <div class="step-title">{{ s.title }}</div>
                <div class="step-out mono">产出：{{ s.output }}</div>
                <div v-if="s.cache" class="step-cache mono">{{ s.cache }}</div>
                <div v-if="s.branch" class="step-branch">分支</div>
              </div>
              <div v-if="!laneSteps(lane.id, ph.id).length" class="phase-empty">（本路径不经过此阶段）</div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 当前步详情 -->
    <div class="card detail-bar" v-if="curStep">
      <div class="detail-left">
        <span class="detail-icon">{{ curStep.icon }}</span>
        <div>
          <div class="detail-title">
            {{ curStep.title }}
            <el-tag size="small" :style="{ color: typeMeta[curStep.type].color, background: typeMeta[curStep.type].bg, borderColor: typeMeta[curStep.type].color }" effect="plain">{{ typeMeta[curStep.type].label }}</el-tag>
            <el-tag size="small" type="info" effect="plain">{{ phaseName(curStep.phase) }}</el-tag>
          </div>
          <div class="detail-desc">{{ curStep.desc }}</div>
        </div>
      </div>
      <div class="detail-right">
        <div class="detail-out"><span class="k">产出</span><span class="v mono">{{ curStep.output }}</span></div>
        <div v-if="curStep.cache" class="detail-out"><span class="k">缓存</span><span class="v mono">{{ curStep.cache }}</span></div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onBeforeUnmount } from 'vue'
import { lanes, phases, steps, paths, typeMeta } from '../mock/flowSteps'

const pathKey = ref('cold')
const curIdx = ref(0)
const playing = ref(false)
let timer = null

const curPath = computed(() => paths[pathKey.value])
const seq = computed(() => curPath.value.ids.map(no => steps.find(s => s.no === no)).filter(Boolean))
const curStep = computed(() => seq.value[curIdx.value])

const lanePhases = (laneId) => phases.filter(p => p.lane === laneId)
const laneSteps = (laneId, phaseId) => seq.value.filter(s => s.lane === laneId && s.phase === phaseId)
const phaseName = (id) => phases.find(p => p.id === id)?.name || id

const stepState = (s) => {
  const pos = seq.value.findIndex(x => x.no === s.no)
  if (!curStep.value) return ''
  if (pos === curIdx.value) return 'current'
  if (pos < curIdx.value) return 'done'
  return 'future'
}
const stepStyle = (s) => {
  const t = typeMeta[s.type]
  return { borderColor: t.color, '--step-color': t.color }
}

const jumpTo = (s) => {
  const pos = seq.value.findIndex(x => x.no === s.no)
  if (pos >= 0) { curIdx.value = pos; stopPlay() }
}

const stepNext = () => {
  if (curIdx.value < seq.value.length - 1) curIdx.value++
  else stopPlay()
}
const stepBack = () => { curIdx.value = Math.max(0, curIdx.value - 1); stopPlay() }
const resetPlay = () => { stopPlay(); curIdx.value = 0 }
const togglePlay = () => {
  if (playing.value) { stopPlay(); return }
  if (curIdx.value >= seq.value.length - 1) curIdx.value = 0
  playing.value = true
  timer = setInterval(() => {
    if (curIdx.value >= seq.value.length - 1) stopPlay()
    else curIdx.value++
  }, 900)
}
const stopPlay = () => { playing.value = false; if (timer) { clearInterval(timer); timer = null } }
onBeforeUnmount(stopPlay)
</script>

<style scoped>
.controls { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.progress { font-size: 12px; color: var(--gray-500); margin-left: auto; display: flex; align-items: center; }
.path-desc { margin-top: 10px; font-size: 12px; color: var(--gray-500); background: var(--gray-50); border-radius: 6px; padding: 8px 10px; }
.legend { display: flex; gap: 14px; margin-top: 10px; flex-wrap: wrap; }
.legend-item { font-size: 11px; color: var(--gray-500); display: flex; align-items: center; gap: 5px; }
.legend-item i { width: 10px; height: 10px; border-radius: 3px; display: inline-block; }

.lanes { margin-top: 16px; display: flex; flex-direction: column; gap: 12px; }
.lane {
  background: #fff;
  border: 1px solid var(--gray-200);
  border-radius: 10px;
  display: flex;
  overflow: hidden;
}
.lane-head {
  width: 150px;
  flex-shrink: 0;
  border-right: 3px solid;
  padding: 14px 12px;
  background: var(--gray-50);
}
.lane-title { font-size: 13px; font-weight: 700; }
.lane-short { font-size: 11px; color: var(--gray-400); margin-top: 4px; }
.lane-body { flex: 1; padding: 10px 12px; }
.lane-phase { display: flex; gap: 8px; }
.lane-phase + .lane-phase { margin-top: 8px; border-top: 1px dashed var(--gray-100); padding-top: 8px; }
.phase-tag {
  width: 34px;
  flex-shrink: 0;
  font-size: 11px;
  font-weight: 700;
  color: var(--gray-500);
  padding-top: 6px;
}
.phase-steps { display: flex; gap: 8px; flex-wrap: wrap; }
.phase-empty { font-size: 11px; color: var(--gray-300); padding: 6px 0; }

.step-card {
  width: 150px;
  border: 2px solid var(--gray-200);
  border-radius: 8px;
  padding: 8px 10px;
  cursor: pointer;
  transition: all .2s;
  background: #fff;
  position: relative;
  opacity: .45;
}
.step-card.done { opacity: .85; background: var(--gray-50); }
.step-card.done::after {
  content: '✓';
  position: absolute;
  top: 6px; right: 8px;
  color: #16a34a;
  font-weight: 700;
  font-size: 12px;
}
.step-card.current {
  opacity: 1;
  background: #fff;
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--step-color) 25%, transparent), 0 4px 12px rgba(0,0,0,.12);
  transform: translateY(-2px);
}
.step-no { font-size: 13px; font-weight: 700; color: var(--step-color, var(--primary)); }
.step-title { font-size: 12px; font-weight: 600; margin-top: 3px; color: var(--gray-800); }
.step-out { font-size: 10px; color: var(--gray-400); margin-top: 4px; word-break: break-all; }
.step-cache { font-size: 10px; color: #15803d; margin-top: 3px; word-break: break-all; }
.step-branch {
  position: absolute;
  top: -10px; right: 8px;
  font-size: 10px;
  color: #b91c1c;
  background: #fee2e2;
  border: 1px solid #fca5a5;
  border-radius: 4px;
  padding: 0 4px;
}

.detail-bar { margin-top: 16px; display: flex; gap: 20px; align-items: center; }
.detail-left { display: flex; gap: 12px; flex: 1; align-items: flex-start; }
.detail-icon {
  width: 40px; height: 40px;
  border-radius: 10px;
  background: var(--primary);
  color: #fff;
  display: flex; align-items: center; justify-content: center;
  font-size: 16px; font-weight: 700;
  flex-shrink: 0;
}
.detail-title { font-size: 14px; font-weight: 700; display: flex; gap: 8px; align-items: center; }
.detail-desc { font-size: 12px; color: var(--gray-600); margin-top: 6px; line-height: 1.6; }
.detail-right { width: 380px; flex-shrink: 0; }
.detail-out { display: flex; gap: 8px; font-size: 12px; padding: 4px 0; }
.detail-out .k { color: var(--gray-500); flex-shrink: 0; }
.detail-out .v { color: var(--gray-800); word-break: break-all; }
</style>

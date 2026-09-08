// 打标引擎 V2 —— 后端 API 封装(/api/tagengine/*; request 层 code===0 已解包 data, 业务错误 reject+ElMessage)
import request from './request'

export const rulesApi = {
  list: (kind) => request.get('/tagengine/rules', { params: kind ? { kind } : {} }),
  get: (id) => request.get(`/tagengine/rules/${id}`),
  create: (body) => request.post('/tagengine/rules', body),
  update: (id, body) => request.put(`/tagengine/rules/${id}`, body),
  remove: (id) => request.delete(`/tagengine/rules/${id}`),
  copy: (id) => request.post(`/tagengine/rules/${id}/copy`)
}

export const registryApi = {
  list: () => request.get('/tagengine/registry'),
  create: (body) => request.post('/tagengine/registry', body),
  update: (id, body) => request.put(`/tagengine/registry/${id}`, body),
  remove: (id) => request.delete(`/tagengine/registry/${id}`)
}
export const claimsApi = {
  list: () => request.get('/tagengine/claims'),
  get: (path) => request.get('/tagengine/claims/x', { params: { path } })
}
export const onlineApi = {
  info: (id) => request.get(`/tagengine/online/${id}`),
  event: (id, body) => request.post(`/tagengine/online/${id}/event`, body),
  entity: (entityKey) => request.get(`/tagengine/online/entity/${entityKey}`)
}
export const streamApi = {
  start: (id) => request.post(`/tagengine/stream/${id}/start`),
  stop: (id) => request.post(`/tagengine/stream/${id}/stop`),
  status: (id) => request.get(`/tagengine/stream/${id}/status`)
}
export const labelsApi = {
  summary: () => request.get('/tagengine/labels/summary'),
  circle: (tagPath, value) => request.get('/tagengine/labels/circle', { params: { tagPath, value } }),
  entity: (entityKey) => request.get(`/tagengine/labels/entity/${entityKey}`)
}

export const subApi = {
  list: () => request.get('/tagengine/subs'),
  create: (body) => request.post('/tagengine/subs', body),
  update: (id, body) => request.put(`/tagengine/subs/${id}`, body),
  remove: (id) => request.delete(`/tagengine/subs/${id}`)
}
export const notifyApi = {
  list: (params) => request.get('/tagengine/notify', { params: params || {} })
}

export const tasksApi = {
  list: () => request.get('/tagengine/tasks'),
  get: (id) => request.get(`/tagengine/tasks/${id}`),
  create: (body) => request.post('/tagengine/tasks', body),
  update: (id, body) => request.put(`/tagengine/tasks/${id}`, body),
  remove: (id) => request.delete(`/tagengine/tasks/${id}`),
  publish: (id) => request.post(`/tagengine/tasks/${id}/publish`),
  run: (id, full, matrix) => request.post(`/tagengine/tasks/${id}/run?trigger=manual${full ? '&full=true' : ''}${matrix ? '&matrix=true' : ''}`)
}

export const runsApi = {
  list: (taskId) => request.get('/tagengine/runs', { params: taskId ? { taskId } : {} }),
  get: (id) => request.get(`/tagengine/runs/${id}`)
}

// 数据集元数据(逻辑表列表/字段; 生产为湖 schema, 当前代理目标库权威/分片表)
export const datasetsApi = {
  list: () => request.get('/tagengine/datasets'),
  fields: (name) => request.get(`/tagengine/datasets/${encodeURIComponent(name)}/fields`)
}

// 标签字典树(tag_dict, 注册表挂节点用): GET /api/tags/tree
export const tagTreeApi = {
  tree: () => request.get('/tags/tree'),
  /** 拍平为 [{id, tag, path}] */
  async flatten() {
    const roots = await this.tree()
    const out = []
    const walk = (nodes, prefix) => {
      ;(nodes || []).forEach((n) => {
        const p = n.path || (prefix ? `${prefix}/${n.tag}` : n.tag)
        out.push({ id: n.id, tag: n.tag, path: p })
        if (n.children && n.children.length) walk(n.children, p)
      })
    }
    walk(roots || [], '')
    return out
  }
}

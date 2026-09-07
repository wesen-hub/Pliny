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

export const tasksApi = {
  list: () => request.get('/tagengine/tasks'),
  get: (id) => request.get(`/tagengine/tasks/${id}`),
  create: (body) => request.post('/tagengine/tasks', body),
  update: (id, body) => request.put(`/tagengine/tasks/${id}`, body),
  remove: (id) => request.delete(`/tagengine/tasks/${id}`),
  publish: (id) => request.post(`/tagengine/tasks/${id}/publish`),
  run: (id) => request.post(`/tagengine/tasks/${id}/run?trigger=manual`)
}

export const runsApi = {
  list: (taskId) => request.get('/tagengine/runs', { params: taskId ? { taskId } : {} }),
  get: (id) => request.get(`/tagengine/runs/${id}`)
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

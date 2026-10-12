// 统一 HTTP 封装：后端语义错误（400/404/409 等）提取 { error, message, conflicts } 结构
const BASE = '/api/v1'

async function request(path, { method = 'GET', body, headers = {} } = {}) {
  const opts = { method, headers: { 'Content-Type': 'application/json', ...headers } }
  if (body !== undefined) opts.body = JSON.stringify(body)

  let res
  try {
    res = await fetch(BASE + path, opts)
  } catch (e) {
    throw new ApiError('NETWORK', '无法连接后端服务，请确认 perm-admin / perm-pdp 已启动', null)
  }

  if (res.status === 204) return null

  const text = await res.text()
  let data = null
  try { data = text ? JSON.parse(text) : null } catch { /* 非 JSON 响应 */ }

  if (!res.ok) {
    const err = data && typeof data === 'object'
      ? { error: data.error || ('HTTP_' + res.status), message: data.message || text, conflicts: data.conflicts || [], blocked: !!data.blocked }
      : { error: 'HTTP_' + res.status, message: text || res.statusText, conflicts: [], blocked: false }
    throw new ApiError(err.error, err.message, err.conflicts, err.blocked)
  }
  return data
}

export class ApiError extends Error {
  constructor(code, message, conflicts = [], blocked = false) {
    super(message)
    this.code = code
    this.conflicts = conflicts
    this.blocked = blocked
  }
}

export const http = {
  get: (path) => request(path),
  post: (path, body, headers) => request(path, { method: 'POST', body, headers }),
  put: (path, body, headers) => request(path, { method: 'PUT', body, headers }),
  del: (path) => request(path, { method: 'DELETE' })
}

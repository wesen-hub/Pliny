// PDP 判定 API（perm-pdp 8081）
import { http } from './http'

/** 判定请求 → 签名产物（模板无值 C1 + HMAC 签名 R-Sec1） */
export const pdpApi = {
  authorize: (req) => http.post('/authorize', req),
  /** PDP 快照版本（展示链路状态用） */
  snapshot: () => http.get('/admin/snapshot'),
  /** PDP 最近审计记录 */
  audit: (limit = 10) => http.get(`/admin/audit?limit=${limit}`)
}

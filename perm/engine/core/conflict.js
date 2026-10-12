/**
 * 约束合并（R-D 系列冲突消解规则）
 * 多条 GRANT 策略同时命中时的约束合并语义：
 *   R-D7  行谓词：AND 连接
 *   R-D8  列可见性：交集 − excludeColumns
 *   R-D9  掩码：同列冲突取最严（FULL > NULL > HASH > PARTIAL > REPLACE）
 *   R-D16 义务：并集，同义务取更强（sync > async）
 *   R-D10 列级短路：可见列为空 → 整表 HIDDEN → DENY
 */
import { MASK_SEVERITY, AUDIT_MODE_SEVERITY } from './model.js';

/** 合并行谓词：多策略 AND 连接（R-D7） */
export function mergeRowPredicates(predicates) {
  const list = predicates.filter((p) => p != null);
  if (list.length === 0) return null; // 无条件 → 不注入
  if (list.length === 1) return list[0];
  return { op: 'AND', children: list };
}

/** 合并列可见性：交集 − exclude（R-D8）；返回 null 表示无列约束（不裁剪） */
export function mergeColumns(grantColumnsList) {
  const constrained = grantColumnsList.filter((c) => c !== null);
  if (constrained.length === 0) return null;
  // 交集
  const first = new Set(constrained[0].columns);
  for (let i = 1; i < constrained.length; i++) {
    for (const col of first) {
      if (!constrained[i].columns.includes(col)) first.delete(col);
    }
  }
  // − excludeColumns
  for (const c of constrained) {
    for (const ex of c.excludeColumns) first.delete(ex);
  }
  return [...first];
}

/** 合并掩码：同列冲突取最严（R-D9） */
export function mergeMasks(maskLists) {
  const map = new Map(); // column -> { type, key }
  for (const masks of maskLists) {
    for (const m of masks) {
      const existing = map.get(m.column);
      if (!existing || MASK_SEVERITY[m.type] > MASK_SEVERITY[existing.type]) {
        map.set(m.column, m);
      }
    }
  }
  return [...map.values()];
}

/** 合并义务：并集，同义务取更强（R-D16） */
export function mergeObligations(obligationLists) {
  const map = new Map(); // type -> mode
  for (const list of obligationLists) {
    for (const o of list) {
      const existing = map.get(o.type);
      if (!existing || AUDIT_MODE_SEVERITY[o.mode] > AUDIT_MODE_SEVERITY[existing]) {
        map.set(o.type, o.mode);
      }
    }
  }
  return [...map.entries()].map(([type, mode]) => ({ type, mode }));
}

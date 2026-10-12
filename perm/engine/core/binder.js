/**
 * PEP 绑定阶段（执行管线 ② ParamBinder）
 * 把渲染占位符的 ref 解析为实际值，产出 bindOrder。
 * 铁律 C1：值唯一出现点——判定链上模板无值，绑定才注入值。
 * fail-closed：任何 ref 解析失败 → 抛 BindError → 判定降为 DENY（R-F1）。
 */

export class BindError extends Error {
  constructor(ref) {
    super(`绑定失败：无法解析引用 ${ref}（fail-closed）`);
    this.name = 'BindError';
    this.ref = ref;
  }
}

/**
 * 解析 subject ref（如 subject.region / subject.roles / subject.userId）
 * @param {string} ref
 * @param {object} context AuthorizationContext
 * @returns {*} 绑定值
 */
function resolveRef(ref, context) {
  const path = ref.split('.');
  if (path[0] !== 'subject') throw new BindError(ref);
  const field = path.slice(1).join('.');
  switch (field) {
    case 'userId':
      return context.userId;
    case 'roles':
      return context.roles;
    case 'tagLevels':
      return context.tagLevels;
    default:
      // attributes 内可嵌套（region / depts / groups ...）
      if (field in context.attributes) return context.attributes[field];
      if (context.attributes[field.split('.')[0]] !== undefined) {
        // 浅层对象查找（如 subject.tagLevels.业务域/风控 走 attributes.tagLevels）
        return context.attributes[field.split('.')[0]];
      }
      throw new BindError(ref);
  }
}

/**
 * 绑定入口：占位符数组 → bindOrder
 * @param {Array} placeholders renderer 输出的占位符
 * @param {object} context AuthorizationContext
 * @returns {Array<{name, value, kind}>} 按占位顺序的绑定值列表
 */
export function bind(placeholders, context) {
  const bindOrder = [];
  for (const ph of placeholders) {
    if (ph.ref == null) {
      // 字面量占位（渲染期直接绑定）
      bindOrder.push({ name: ph.name, value: ph.literal, kind: ph.kind });
      continue;
    }
    const value = resolveRef(ph.ref, context);
    if (ph.kind === 'list' && !Array.isArray(value)) {
      throw new BindError(`${ph.ref} 应为列表`);
    }
    bindOrder.push({ name: ph.name, value, kind: ph.kind });
  }
  return bindOrder;
}

/**
 * PEP 注入与投影阶段（执行管线 ③ PredicateInjector + ④ ColumnPruner）
 * ③ 谓词注入：权限谓词注入 WHERE；权限 ? 一律追加在原业务 ? 之后（bindOrder 顺序契约）
 * ④ 列投影：SELECT * 展开 + 可见列裁剪（R-D8）+ 掩码列表达式替换（R-D9）
 *
 * 说明：v1 核心引擎用字符串级简化解析（演示级），生产实现由各接入适配器
 * （MyBatis 拦截器 / JDBC 代理）基于 AST 解析器（如 JSqlParser）完成同等语义。
 */

/** 简化 SQL 解析：拆出 select 列表与 WHERE 之后的尾部 */
function parseSql(sql) {
  const m = sql.match(/^select\s+([\s\S]*?)\s+from\s+(\S+)([\s\S]*)$/i);
  if (!m) throw new Error(`无法解析 SQL（演示级解析器仅支持简单 SELECT）: ${sql}`);
  return { selectList: m[1].trim(), table: m[2], tail: m[3].trim() };
}

/**
 * ③ 谓词注入
 * @param {string} sql 原始业务 SQL
 * @param {string|null} permPredicate 权限谓词 SQL 文本（含 ? 占位，list 已展开）
 * @returns {string} 改写后 SQL（权限谓词注入 WHERE；无 WHERE 自动补）
 */
export function injectWhere(sql, permPredicate) {
  if (!permPredicate) return sql;
  const m = sql.match(/^([\s\S]*?\sfrom\s+\S+\s*)(where\s+)([\s\S]*)$/i);
  if (m) {
    // 原 WHERE 保留，权限谓词 AND 追加（原 ? 在前，权限 ? 在后）
    return `${m[1]}WHERE (${permPredicate}) AND (${m[3]})`;
  }
  const m2 = sql.match(/^([\s\S]*?\sfrom\s+\S+\s*)([\s\S]*)$/i);
  return `${m2[1]}WHERE ${permPredicate} ${m2[2]}`;
}

/**
 * 把渲染模板 + 绑定值拼成最终权限谓词 SQL（占位展开）
 * @param {string} template 含 :p0 / :list0 占位的模板
 * @param {Array} bindOrder binder.bind 输出
 * @returns {string} ? 占位 SQL 文本
 */
export function renderBoundPredicate(template, bindOrder) {
  let sql = template;
  for (const b of bindOrder) {
    const ph = b.kind === 'list' ? b.name : b.name;
    if (b.kind === 'list') {
      const marks = b.value.map(() => '?').join(', ');
      sql = sql.replace(ph, marks);
    } else {
      sql = sql.replace(ph, '?');
    }
  }
  return sql;
}

/**
 * ④ 列投影：SELECT * 展开 + 裁剪 + 掩码替换
 * @param {string} sql 注入后的 SQL
 * @param {import('./model.js').Artifact} artifact 判定产物
 * @param {string[]} schemaColumns 表全列（资源元数据，用于 SELECT * 展开）
 * @returns {string} 投影后 SQL
 */
export function projectColumns(sql, artifact, schemaColumns) {
  const { visibleColumns, masks } = artifact;
  const maskMap = new Map(masks.map((m) => [m.column, m]));
  const parsed = parseSql(sql);

  // SELECT * 展开为全列；否则解析现有列表（逗号拆分，演示级）
  let cols = parsed.selectList === '*'
    ? [...schemaColumns]
    : parsed.selectList.split(',').map((s) => s.trim());

  // 可见列裁剪（R-D8）；visibleColumns 为 null = 不裁剪
  if (visibleColumns !== null) {
    cols = cols.filter((c) => {
      const name = c.replace(/^`|`$/g, '').split(/\s+/)[0];
      return visibleColumns.includes(name);
    });
  }

  // 掩码列替换（R-D9）：FULL 剔除；其余替换表达式
  const projected = cols.flatMap((c) => {
    const name = c.replace(/^`|`$/g, '').split(/\s+/)[0];
    const mask = maskMap.get(name);
    if (!mask) return [c];
    if (mask.type === 'FULL') return [];
    return [renderMaskExpr(name, mask)];
  });

  return `SELECT ${projected.join(', ')} FROM ${parsed.table}${parsed.tail ? ` ${parsed.tail}` : ''}`;
}

/** 掩码表达式（注入阶段复用 renderer 的方言函数） */
function renderMaskExpr(column, mask) {
  const c = `\`${column}\``;
  switch (mask.type) {
    case 'NULL': return `NULL AS ${c}`;
    case 'HASH': return `SHA2(${c}, 256) AS ${c}`;
    case 'PARTIAL': return `CONCAT(LEFT(${c}, 3), '****') AS ${c}`;
    case 'REPLACE': return `'***' AS ${c}`;
    default: throw new Error(`未实现的掩码类型: ${mask.type}`);
  }
}

/**
 * 标签模式 A 列表下推（跨库定稿 R-T5）：实体列表 → customer_id IN (?,...)
 * 校验式（verify）不产谓词，只返回授权声明（权限锚定在圈选动作）。
 */
export function renderTagPredicate(tagScope) {
  if (!tagScope) return null;
  if (tagScope.expandMode === 'verify') return null;
  if (tagScope.expandMode !== 'list' || !tagScope.entities || tagScope.entities.length === 0) {
    return null;
  }
  const marks = tagScope.entities.map(() => '?').join(', ');
  return `\`${tagScope.targetKey}\` IN (${marks})`;
}

/** 提取标签实体绑定值（追加到 bindOrder 尾部） */
export function tagBindValues(tagScope) {
  if (!tagScope || tagScope.expandMode !== 'list') return [];
  return tagScope.entities;
}

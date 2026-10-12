/**
 * 方言渲染器（PDP 翻译阶段 ㉒）：行谓词 AST → SQL 模板 + 占位符
 * 铁律 C1：模板无值——渲染只产占位符与绑定引用，值在 PEP 绑定阶段才出现。
 * 本模块实现 mysql 方言；trino/es/neo4j/spark/flink 等由其他 Renderer 提供（SPI 扩展点）。
 *
 * 占位符模型：
 *   { name: ':p0', ref: 'subject.region', kind: 'scalar' }   —— 单值绑定
 *   { name: ':list0', ref: 'subject.depts', kind: 'list' }   —— 列表绑定（注入阶段展开为 n 个 ?）
 */
export class RenderError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'RenderError';
  }
}

const quote = (col) => `\`${col}\``;

/**
 * 渲染行谓词 → { template, placeholders }
 * @param {object} predicate 行谓词 AST（model.js 构造辅助产物）
 */
export function renderPredicate(predicate) {
  const placeholders = [];
  const template = renderNode(predicate, placeholders);
  return { template, placeholders };
}

function renderNode(node, placeholders) {
  switch (node.op) {
    case 'CONST':
      return node.value ? '1=1' : '1=0';
    case 'EQ':
      return `${quote(node.column)} = ${bind(node.value, placeholders)}`;
    case 'IN':
      return `${quote(node.column)} IN (${bindList(node.value, placeholders)})`;
    case 'RANGE': {
      const parts = [];
      if (node.min != null) parts.push(`${quote(node.column)} >= ${bind(node.min, placeholders)}`);
      if (node.max != null) parts.push(`${quote(node.column)} <= ${bind(node.max, placeholders)}`);
      if (parts.length === 0) throw new RenderError('RANGE 节点 min/max 均为空');
      return parts.join(' AND ');
    }
    case 'REGEX':
      return `${quote(node.column)} REGEXP ${bind({ type: 'literal', value: node.pattern }, placeholders)}`;
    case 'AND':
      return `(${node.children.map((c) => renderNode(c, placeholders)).join(' AND ')})`;
    case 'OR':
      return `(${node.children.map((c) => renderNode(c, placeholders)).join(' OR ')})`;
    default:
      throw new RenderError(`未实现的行谓词节点: ${node.op}`);
  }
}

/** 单值绑定：ref → 占位符；literal → 绑定为参数（值不进 SQL 文本，R-Sec2） */
function bind(value, placeholders) {
  if (value.type === 'literal') {
    const name = `:p${placeholders.length}`;
    placeholders.push({ name, ref: null, kind: 'scalar', literal: value.value });
    return '?';
  }
  const name = `:p${placeholders.length}`;
  placeholders.push({ name, ref: value.ref, kind: 'scalar' });
  return '?';
}

/** 列表绑定：占位标记 :listN，注入阶段按绑定数组长度展开为 n 个 ? */
function bindList(value, placeholders) {
  const name = `:list${placeholders.length}`;
  placeholders.push({ name, ref: value.ref, kind: 'list' });
  return name;
}

// ============ 掩码渲染（mysql 方言） ============

/**
 * 渲染掩码列投影表达式
 * @param {string} column
 * @param {{type: string, key?: string}} mask 掩码类型（HASH/NULL/PARTIAL/REPLACE）
 * @returns {string} mysql 表达式（FULL 由投影器剔除，不进本函数）
 */
export function renderMask(column, mask) {
  const c = quote(column);
  switch (mask.type) {
    case 'NULL':
      return `NULL AS ${c}`;
    case 'HASH':
      return `SHA2(${c}, 256) AS ${c}`;
    case 'PARTIAL':
      return `CONCAT(LEFT(${c}, 3), '****') AS ${c}`;
    case 'REPLACE':
      return `'***' AS ${c}`;
    default:
      throw new RenderError(`未实现的掩码类型: ${mask.type}`);
  }
}

// 数据库类型 → 实例到表的层级结构(不同数据库的 database/schema 概念不同)
//   MySQL/MongoDB: 实例 → 数据库 → 表/集合
//   PostgreSQL/SQL Server: 实例 → 数据库 → Schema → 表
//   Oracle: 实例 → Schema(用户) → 表(无 database 层)
export const DB_LEVELS = {
  mysql:      { database: true,  schema: false, dbLabel: '数据库',      tableLabel: '表' },
  postgresql: { database: true,  schema: true,  dbLabel: '数据库',      tableLabel: '表' },
  sqlserver:  { database: true,  schema: true,  dbLabel: '数据库',      tableLabel: '表' },
  oracle:     { database: false, schema: true,  dbLabel: 'Schema(用户)', tableLabel: '表' },
  mongodb:    { database: true,  schema: false, dbLabel: '数据库',      tableLabel: '集合' },
  s3:         { database: true,  schema: false, dbLabel: '桶(bucket)', tableLabel: '对象' },
  local:      { database: true,  schema: false, dbLabel: '目录',       tableLabel: '文件' }
}

export const DB_TYPE_OPTIONS = [
  { value: 'mysql', label: 'MySQL' },
  { value: 'postgresql', label: 'PostgreSQL' },
  { value: 'oracle', label: 'Oracle' },
  { value: 'sqlserver', label: 'SQL Server' },
  { value: 'mongodb', label: 'MongoDB' },
  { value: 's3', label: 'S3 / MinIO(文件)' },
  { value: 'local', label: '本地目录(客户端代理)' }
]

export const DEFAULT_PORTS = { mysql: 3306, postgresql: 5432, oracle: 1521, sqlserver: 1433, mongodb: 27017, s3: 9000, local: 0 }

export function dbLevels(type) {
  return DB_LEVELS[type] || DB_LEVELS.mysql
}

// 业务主键「来源维度」选项,按数据源类型动态生成
// kind: biz=业务身份(推荐) / source=来源级定位 / physical=物理身份(换库/迁移会变,需确认)
export function scopeFieldOptions(type) {
  const lv = dbLevels(type)
  const opts = [{ value: 'system_name', label: '系统名称', kind: 'biz' }]
  if (lv.database) opts.push({ value: 'database_name', label: `${lv.dbLabel}名称`, kind: 'physical' })
  if (lv.schema) opts.push({ value: 'schema_name', label: 'Schema 名称', kind: 'physical' })
  opts.push({ value: 'datasource_id', label: '数据源编号', kind: 'physical' })
  opts.push({ value: 'table_name', label: `${lv.tableLabel}名`, kind: 'source' })
  return opts
}

// 物理身份字段:进键会导致 换库/主从切换/实例迁移 → 实体分裂,必须显式确认
export const PHYSICAL_FIELDS = ['datasource_id', 'database_name', 'schema_name']

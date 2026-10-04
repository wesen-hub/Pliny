package com.dcp.sync;

import com.dcp.engine.ExpressionUtil;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 同步任务配置服务(实现 1): 向导「字段映射 + 行级转换 + 业务主键」提交的后端化。
 *
 * <ul>
 *   <li>validate: Aviator 表达式安全校验 + 字段引用检查(复用引擎 ExpressionUtil)</li>
 *   <li>save: 完整配置落元库 sync_task(JSON)</li>
 *   <li>字段流唯一性: 同一(数据源,源库,源表,源列)只能有一条采集流(跨任务唯一, 防重复采集同一列);
 *       登记表 sync_field_flow; 业务键重叠**不做校验**(不可静态判定) —— 重叠时按“后写覆盖先写”(零仲裁)</li>
 * </ul>
 */
@Slf4j
@Service
public class SyncTaskService {

    private final JdbcTemplate metaJdbc;

    public SyncTaskService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    /**
     * 校验字段映射与行级转换配置。
     *
     * @param fieldMapping 前端 fieldMapping:[{modelField, source:{kind, srcField, expr, ...}, steps:[...]}]
     * @param availableFields 可引用字段(源字段 + 前置模型字段)
     * @return 问题列表(空 = 通过)
     */
    public List<String> validate(List<Map<String, Object>> fieldMapping, List<String> availableFields) {
        // 结构强校验(非空/D2/保留列/kind 约束): 与直建路径共用同一套规则
        List<String> issues = new ArrayList<>(SyncMappingRules.validateStructure(fieldMapping));
        if (fieldMapping == null) {
            return issues;
        }
        var seen = new java.util.HashSet<String>();
        if (availableFields != null) {
            seen.addAll(availableFields);
        }
        for (Map<String, Object> row : fieldMapping) {
            String modelField = String.valueOf(row.get("modelField"));
            Map<String, Object> source = (Map<String, Object>) row.getOrDefault("source", Map.of());
            String kind = String.valueOf(source.getOrDefault("kind", ""));
            if (kind.isBlank() || "null".equals(kind)) {
                issues.add("字段 " + modelField + " 未定义来源");
                continue;
            }
            if ("field-map".equals(kind)) {
                String srcField = String.valueOf(source.getOrDefault("srcField", ""));
                // availableFields 为 null = 后端无源表字段信息(源字段由采集端读取), 跳过引用检查, 信任前端级联选择
                if (availableFields != null && !seen.contains(srcField)) {
                    issues.add("字段 " + modelField + " 引用了未知源字段: " + srcField);
                }
            }
            if ("compute".equals(kind)) {
                String expr = String.valueOf(source.getOrDefault("expr", ""));
                try {
                    String err = ExpressionUtil.validate(expr, seen);
                    if (err != null && !err.isBlank()) {
                        issues.add("字段 " + modelField + " 表达式错误: " + err);
                    }
                } catch (Exception e) {
                    issues.add("字段 " + modelField + " 表达式编译失败: " + e.getMessage());
                }
            }
            seen.add(modelField); // 后者可引用前者输出(拓扑序)
        }
        return issues;
    }

    /** 保存同步任务配置(向导提交; 多源表 = 多表合并: 每源表注册一个 SeaTunnel 采集任务) */
    public Long save(String name, String logicalTable, Long datasourceId, String sourceDb,
                     List<String> sourceTables, List<Map<String, Object>> fieldMapping,
                     Object rowChain, Object businessKey,
                     String syncMode, String fullScope, String cronExpr, String startMode, String startValue) {
        return save(name, logicalTable, datasourceId, sourceDb, sourceTables, fieldMapping,
                rowChain, businessKey, syncMode, fullScope, cronExpr, startMode, startValue, null, false);
    }

    /**
     * 保存同步任务配置(可跨数据源/库)。
     *
     * @param originByTable 可选: 源表名 → {datasourceId, sourceDb}(多数据源/多库支持);
     *                      缺省回退 datasourceId/sourceDb。字段映射按 srcTable 归属拆分到各源表任务。
     * @param newInstance   true=创建独立新任务实例(同(逻辑,源)可与旧任务并存, 各自 seq/写者列, 后任务覆盖先任务;
     *                      需自行先停旧实例); false=更新该逻辑表现有任务(编辑/补字段/改映射, 原位升级, 不新建实例)。
     */
    public Long save(String name, String logicalTable, Long datasourceId, String sourceDb,
                     List<String> sourceTables, List<Map<String, Object>> fieldMapping,
                     Object rowChain, Object businessKey,
                     String syncMode, String fullScope, String cronExpr, String startMode, String startValue,
                     Map<String, Map<String, Object>> originByTable, boolean newInstance) {
        // 注册强校验(新方案): 映射非空/D2/保留列/compute 表达式, UI 与直建路径统一, 失败不留半成品
        List<String> issues = validate(fieldMapping, null);
        if (!issues.isEmpty()) {
            throw new IllegalArgumentException("字段映射校验失败: " + String.join("; ", issues));
        }
        // 行级转换(仅 filter/filter-kind): 按源表拆分, 随 job 配置下发采集端执行
        @SuppressWarnings("unchecked")
        Map<String, Object> chainByTable = (Map<String, Object>) (rowChain instanceof Map<?, ?> m ? m : Map.of());
        String fm = toJson(fieldMapping);
        String rc = rowChain == null ? null : toJson(rowChain);
        String bk = businessKey == null ? null : toJson(businessKey);
        String firstSource = sourceTables == null || sourceTables.isEmpty() ? null : sourceTables.get(0);
        // 任务行归属: 默认更新该逻辑表现有任务(编辑/升级原位, 不叠行); newInstance=true 或尚无任务 → 新插一行(新实例身份)
        Long ownerId = null;
        if (!newInstance) {
            try {
                Long maxId = metaJdbc.queryForObject(
                        "SELECT MAX(id) FROM sync_task WHERE logical_table = ?", Long.class, logicalTable);
                if (maxId != null) {
                    ownerId = maxId;
                }
            } catch (Exception ignore) {
                // 无任务行
            }
        }
        // 字段流唯一性(跨任务): 同一(数据源,库,源表,源列)只能有一条采集流 —— 写库前先校验, 失败不留半成品
        List<Map<String, Object>> flows = flowsOf(logicalTable, fieldMapping, datasourceId, sourceDb, originByTable);
        List<String> flowIssues = validateFieldFlows(flows, ownerId);
        if (!flowIssues.isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", flowIssues));
        }
        if (ownerId == null) {
            metaJdbc.update(
                    "INSERT INTO sync_task (name, logical_table, datasource_id, source_db, source_table, "
                            + "field_mapping, row_chain, business_key, status, sync_mode, full_scope, cron_expr, start_mode, start_value) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'SUBMITTED', ?, ?, ?, ?, ?)",
                    name, logicalTable, datasourceId, sourceDb, firstSource, fm, rc, bk,
                    syncMode == null ? "full-cdc" : syncMode, fullScope, cronExpr,
                    startMode == null ? "earliest" : startMode, startValue);
            ownerId = metaJdbc.queryForObject("SELECT LAST_INSERT_ID()", Long.class);
        } else {
            metaJdbc.update(
                    "UPDATE sync_task SET name = ?, datasource_id = ?, source_db = ?, source_table = ?, "
                            + "field_mapping = ?, row_chain = ?, business_key = ?, status = 'SUBMITTED', "
                            + "sync_mode = ?, full_scope = ?, cron_expr = ?, start_mode = ?, start_value = ? WHERE id = ?",
                    name, datasourceId, sourceDb, firstSource, fm, rc, bk,
                    syncMode == null ? "full-cdc" : syncMode, fullScope, cronExpr,
                    startMode == null ? "earliest" : startMode, startValue, ownerId);
        }
        // 多表合并: 每个源表注册一个 SeaTunnel 采集任务(字段映射按源表拆分, 随 job 配置下发到采集端)
        if (sourceTables != null && !sourceTables.isEmpty()) {
            List<String> keyFields = businessKeyKeyFields(businessKey, logicalTable);
            String topic = logicalTableTopic(logicalTable);
            for (String t : sourceTables) {
                String perTableMapping = toJson(fieldMapping.stream()
                        .filter(m -> {
                            Object st = m.get("source") instanceof Map<?, ?> src ? src.get("srcTable") : null;
                            return st == null || String.valueOf(st).equals(t);
                        })
                        .toList());
                Object perTableChain = chainByTable.get(t);
                // 多数据源/多库: 每张源表可指向自己的数据源与库(originByTable 优先, 缺省回退任务级默认)
                Map<String, Object> origin = originByTable == null ? null : originByTable.get(t);
                Long dsId = origin == null ? datasourceId
                        : ((Number) origin.getOrDefault("datasourceId", datasourceId)).longValue();
                String db = origin == null ? sourceDb
                        : String.valueOf(origin.getOrDefault("sourceDb", sourceDb));
                metaJdbc.update(
                        "INSERT INTO cdc_st_task (logical_table, datasource_id, source_db, source_table, topic, "
                                + "business_key_fields, key_columns, field_mapping, row_chain, sync_task_id) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                                + "ON DUPLICATE KEY UPDATE datasource_id=VALUES(datasource_id), source_db=VALUES(source_db), "
                                + "topic=VALUES(topic), business_key_fields=VALUES(business_key_fields), "
                                + "key_columns=VALUES(key_columns), field_mapping=VALUES(field_mapping), "
                                + "row_chain=VALUES(row_chain)",
                        logicalTable, dsId, db, t, topic, toJson(keyFields), toJson(keyFields),
                        perTableMapping, perTableChain == null ? null : toJson(perTableChain), ownerId);
            }
        }
        // 多写者提示(新方案, 零阻断): 同逻辑表内同 modelField 已被其他任务写入 → 信息性提示
        // (跨源表/跨任务同 modelField 属业务允许; 同主键撞行时按 LWW 后写覆盖)
        for (Map<String, Object> f : flows) {
            List<Map<String, Object>> peers = metaJdbc.queryForList(
                    "SELECT sync_task_id FROM sync_field_flow WHERE logical_table = ? AND target_col = ? "
                            + "AND sync_task_id <> ? LIMIT 1",
                    f.get("logicalTable"), f.get("targetCol"), ownerId == null ? -1 : ownerId);
            if (!peers.isEmpty()) {
                log.info("提示: modelField {} 已被其他任务(#{})写入, 同主键撞行时按 LWW 后写覆盖",
                        f.get("targetCol"), peers.get(0).get("sync_task_id"));
            }
        }
        refreshFieldFlows(ownerId, flows);   // 本任务字段流登记(先删后插, 含编辑/补字段)
        log.info("同步任务已保存 id={} name={} logicalTable={} sources={} newInstance={} 字段流={} 条",
                ownerId, name, logicalTable, sourceTables, newInstance, flows.size());
        return ownerId;
    }

    // ───────────────── 字段流注册: 同一源列只能有一条采集流(跨任务唯一) ─────────────────

    /** 启动自愈: 建登记表(幂等) + 从存量 sync_task 回填登记(幂等, 冲突保留先到) */
    @PostConstruct
    public void initFieldFlows() {
        try {
            metaJdbc.execute("CREATE TABLE IF NOT EXISTS sync_field_flow ("
                    + "id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
                    + "logical_table VARCHAR(128) NOT NULL,"
                    + "target_col VARCHAR(128) NOT NULL,"
                    + "datasource_id BIGINT NOT NULL,"
                    + "source_db VARCHAR(64) NOT NULL DEFAULT '',"
                    + "source_table VARCHAR(64) NOT NULL,"
                    + "source_col VARCHAR(128) NOT NULL,"
                    + "sync_task_id BIGINT UNSIGNED NOT NULL,"
                    + "update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "UNIQUE KEY uk_flow (datasource_id, source_db, source_table, source_col),"
                    + "KEY idx_task (sync_task_id)) ENGINE=InnoDB"
                    + " COMMENT='字段流注册: 同一源列只能有一条采集流(跨任务唯一)'");
        } catch (Exception e) {
            log.warn("字段流登记表初始化失败: {}", e.getMessage());
        }
        backfillFieldFlows();
    }

    /** 提取“字段流”: 仅 kind=field-map 且源表/源列非空的行(一条边 = 一个源列 → 一个目标列) */
    private List<Map<String, Object>> flowsOf(String logicalTable, List<Map<String, Object>> fieldMapping,
                                              Long defaultDsId, String defaultDb,
                                              Map<String, Map<String, Object>> originByTable) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (fieldMapping == null) {
            return out;
        }
        for (Map<String, Object> m : fieldMapping) {
            Object srcObj = m.get("source");
            if (!(srcObj instanceof Map<?, ?> src)) continue;
            if (!"field-map".equals(String.valueOf(src.get("kind")))) continue;   // compute/uuid/system 不占流
            String srcTable = trimStr(src.get("srcTable"));
            String srcField = trimStr(src.get("srcField"));
            String target = trimStr(m.get("modelField"));
            if (srcTable.isEmpty() || srcField.isEmpty() || target.isEmpty()) continue;
            Map<String, Object> o = originByTable == null ? null : originByTable.get(srcTable);
            long dsId = o == null
                    ? (defaultDsId == null ? 0L : defaultDsId)
                    : ((Number) o.get("datasourceId")).longValue();
            String db = o == null
                    ? (defaultDb == null ? "" : defaultDb)
                    : String.valueOf(o.get("sourceDb"));
            Map<String, Object> f = new LinkedHashMap<>();
            f.put("logicalTable", logicalTable);
            f.put("targetCol", target);
            f.put("datasourceId", dsId);
            f.put("sourceDb", db == null ? "" : db);
            f.put("sourceTable", srcTable);
            f.put("sourceCol", srcField);
            out.add(f);
        }
        return out;
    }

    /** 字段流唯一性校验: 同一(数据源,库,源表,源列)全局只能有一条采集流; selfTaskId=编辑时自身豁免 */
    public List<String> validateFieldFlows(List<Map<String, Object>> flows, Long selfTaskId) {
        List<String> issues = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Map<String, Object> f : flows) {
            String key = f.get("datasourceId") + "|" + f.get("sourceDb") + "|" + f.get("sourceTable") + "|" + f.get("sourceCol");
            if (!seen.add(key)) {
                issues.add("字段流重复: 同一次提交里源列 " + f.get("sourceTable") + "." + f.get("sourceCol")
                        + " 被映射到多个目标列(已见 " + f.get("targetCol") + ")");
                continue;
            }
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT sync_task_id, logical_table, target_col FROM sync_field_flow "
                            + "WHERE datasource_id = ? AND source_db = ? AND source_table = ? AND source_col = ?",
                    f.get("datasourceId"), f.get("sourceDb"), f.get("sourceTable"), f.get("sourceCol"));
            for (Map<String, Object> r : rows) {
                long owner = ((Number) r.get("sync_task_id")).longValue();
                if (selfTaskId != null && owner == selfTaskId) {
                    continue;
                }
                issues.add("源列 " + f.get("sourceTable") + "." + f.get("sourceCol") + " 已被任务#" + owner
                        + " 采集到逻辑表 " + r.get("logical_table") + "." + r.get("target_col")
                        + ": 同一源列只能有一条采集流(请改用其他源列, 或先停用/删除原任务)");
                break;
            }
        }
        return issues;
    }

    /** 刷新本任务的字段流登记(先删后插; 唯一键兼容并发抢注) */
    private void refreshFieldFlows(long taskId, List<Map<String, Object>> flows) {
        try {
            metaJdbc.update("DELETE FROM sync_field_flow WHERE sync_task_id = ?", taskId);
            for (Map<String, Object> f : flows) {
                metaJdbc.update("INSERT INTO sync_field_flow (logical_table, target_col, datasource_id, "
                                + "source_db, source_table, source_col, sync_task_id) VALUES (?,?,?,?,?,?,?)",
                        f.get("logicalTable"), f.get("targetCol"), f.get("datasourceId"),
                        f.get("sourceDb"), f.get("sourceTable"), f.get("sourceCol"), taskId);
            }
        } catch (Exception e) {
            log.warn("字段流登记刷新失败(task={}): {}", taskId, e.getMessage());
        }
    }

    /** 字段流查询(排查/血缘用): 按任务或按逻辑表 */
    public List<Map<String, Object>> fieldFlows(Long taskId, String logicalTable) {
        if (taskId != null) {
            return metaJdbc.queryForList("SELECT * FROM sync_field_flow WHERE sync_task_id = ? ORDER BY source_table, source_col", taskId);
        }
        return metaJdbc.queryForList("SELECT * FROM sync_field_flow WHERE logical_table = ? ORDER BY source_table, source_col", logicalTable);
    }

    /** 存量回填: sync_task.field_mapping → 登记表(仅 field-map; 已登记的任务跳过; 冲突保留先到) */
    @SuppressWarnings("unchecked")
    private void backfillFieldFlows() {
        try {
            List<Map<String, Object>> tasks = metaJdbc.queryForList(
                    "SELECT id, logical_table, datasource_id, source_db, field_mapping FROM sync_task");
            int added = 0, conflict = 0;
            for (Map<String, Object> t : tasks) {
                if (t.get("field_mapping") == null) continue;
                long tid = ((Number) t.get("id")).longValue();
                if (!metaJdbc.queryForList("SELECT id FROM sync_field_flow WHERE sync_task_id = ? LIMIT 1", tid).isEmpty()) {
                    continue;   // 已登记(不覆盖运行中任务的最新映射)
                }
                List<Map<String, Object>> mapping;
                try {
                    mapping = new com.fasterxml.jackson.databind.ObjectMapper()
                            .readValue(String.valueOf(t.get("field_mapping")), List.class);
                } catch (Exception e) {
                    continue;
                }
                Long ds = t.get("datasource_id") == null ? 0L : ((Number) t.get("datasource_id")).longValue();
                String db = t.get("source_db") == null ? "" : String.valueOf(t.get("source_db"));
                for (Map<String, Object> f : flowsOf(String.valueOf(t.get("logical_table")), mapping, ds, db, null)) {
                    try {
                        metaJdbc.update("INSERT INTO sync_field_flow (logical_table, target_col, datasource_id, "
                                        + "source_db, source_table, source_col, sync_task_id) VALUES (?,?,?,?,?,?,?)",
                                f.get("logicalTable"), f.get("targetCol"), f.get("datasourceId"),
                                f.get("sourceDb"), f.get("sourceTable"), f.get("sourceCol"), tid);
                        added++;
                    } catch (Exception dup) {
                        conflict++;
                    }
                }
            }
            if (added > 0 || conflict > 0) {
                log.info("[字段流] 存量回填: 新增 {} 条, 冲突 {} 条(冲突=同一源列被多个存量任务采集, 保留先到)", added, conflict);
            }
        } catch (Exception e) {
            log.warn("字段流回填异常: {}", e.getMessage());
        }
    }

    private static String trimStr(Object o) {
        return o == null ? "" : String.valueOf(o).trim();
    }

    /** 业务键字段: 优先取提交的 businessKey.keyFields, 否则读 logical_table 已存 key_columns */
    @SuppressWarnings("unchecked")
    private List<String> businessKeyKeyFields(Object businessKey, String logicalTable) {
        if (businessKey instanceof Map<?, ?> bk) {
            Object kf = bk.get("keyFields");
            if (kf instanceof List<?> list && !list.isEmpty()) {
                return list.stream().map(String::valueOf).toList();
            }
        }
        try {
            String kc = metaJdbc.queryForObject(
                    "SELECT key_columns FROM logical_table WHERE table_name = ?", String.class, logicalTable);
            if (kc != null && !kc.isBlank()) {
                return new com.fasterxml.jackson.databind.ObjectMapper().readValue(kc, List.class);
            }
        } catch (Exception ignore) {
            // 未配置则回退
        }
        return List.of();
    }

    private String logicalTableTopic(String logicalTable) {
        try {
            return metaJdbc.queryForObject(
                    "SELECT topic FROM logical_table WHERE table_name = ?", String.class, logicalTable);
        } catch (Exception e) {
            return "dcp." + logicalTable;
        }
    }

    /**
     * 多表合并键约束(方案 1): merge 模式禁止物理身份(数据源/库/Schema)与表名进键;
     * 这些维度进键会导致同实体跨源合不到一行 / 换库分裂。
     */
    public List<String> validateMergeKey(Map<String, Object> businessKey) {
        List<String> issues = new ArrayList<>();
        if (businessKey == null) {
            return issues;
        }
        String mode = String.valueOf(businessKey.getOrDefault("mode", "merge"));
        if (!"merge".equals(mode)) {
            return issues; // scope(区分)模式不合并, 不适用本约束
        }
        @SuppressWarnings("unchecked")
        List<String> scopeFields = (List<String>) businessKey.get("scopeFields");
        if (scopeFields != null) {
            for (String f : scopeFields) {
                if (List.of("datasource_id", "database_name", "schema_name", "table_name").contains(f)) {
                    issues.add("merge(合并)模式禁止来源维度进键: " + f
                            + "(物理身份/表名进键 = 同实体跨源分裂, 无法合并)");
                }
            }
        }
        return issues;
    }

    private static String toJson(Object o) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 序列化失败", e);
        }
    }
}

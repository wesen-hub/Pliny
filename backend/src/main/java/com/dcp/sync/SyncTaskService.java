package com.dcp.sync;

import com.dcp.engine.ExpressionUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 同步任务配置服务(实现 1): 向导「字段映射 + 行级转换 + 业务主键」提交的后端化。
 *
 * <ul>
 *   <li>validate: Aviator 表达式安全校验 + 字段引用检查(复用引擎 ExpressionUtil)</li>
 *   <li>save: 完整配置落元库 sync_task(JSON)</li>
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
        List<String> issues = new ArrayList<>();
        if (fieldMapping == null) {
            return List.of("字段映射为空");
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
        log.info("同步任务已保存 id={} name={} logicalTable={} sources={} newInstance={}",
                ownerId, name, logicalTable, sourceTables, newInstance);
        return ownerId;
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

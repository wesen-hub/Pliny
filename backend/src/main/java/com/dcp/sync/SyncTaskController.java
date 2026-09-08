package com.dcp.sync;

import org.springframework.security.access.prepost.PreAuthorize;

import com.dcp.common.ApiResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 同步任务配置 API(实现 1):
 *
 * <pre>
 *   GET  /api/sync/tasks           任务列表(源表按逻辑表聚合, 含采集运行状态)
 *   POST /api/sync/tasks/validate  校验字段映射(表达式/引用)
 *   POST /api/sync/tasks           保存向导完整配置(字段映射+行级链+业务主键; 多源表 = 多表合并, 自动注册采集任务)
 * </pre>
 */
@RestController
@RequestMapping("/api/sync/tasks")
public class SyncTaskController {

    private final SyncTaskService service;
    private final SeaTunnelCdcService seaTunnelCdcService;
    private final JdbcTemplate metaJdbc;

    public SyncTaskController(SyncTaskService service, SeaTunnelCdcService seaTunnelCdcService,
                              JdbcTemplate metaJdbc) {
        this.service = service;
        this.seaTunnelCdcService = seaTunnelCdcService;
        this.metaJdbc = metaJdbc;
    }

    /** 任务列表: sync_task 维度, 源表 = 该逻辑表下 cdc_task 全部源表(多表合并聚合展示) */
    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        try {
            // 任务 = 逻辑表(一任务多源表共享模式); 模式配置在 logical_table(任务级)
            List<Map<String, Object>> tables = metaJdbc.queryForList(
                    "SELECT lt.table_name, lt.sync_mode, lt.full_scope, lt.cron_expr, lt.start_mode, "
                            + "lt.run_status, lt.last_run_time, "
                            + "(SELECT st.name FROM sync_task st WHERE st.logical_table = lt.table_name ORDER BY st.id DESC LIMIT 1) AS task_name, "
                            + "(SELECT COUNT(*) FROM cdc_st_task ct2 WHERE ct2.logical_table = lt.table_name) AS cdc_count "
                            + "FROM logical_table lt "
                            + "WHERE EXISTS (SELECT 1 FROM cdc_st_task ct3 WHERE ct3.logical_table = lt.table_name) "
                            + "ORDER BY lt.table_name");
            for (Map<String, Object> t : tables) {
                Map<String, Object> row = new LinkedHashMap<>();
                String logicalTable = String.valueOf(t.get("table_name"));
                row.put("logical_table", logicalTable);
                row.put("name", t.get("task_name") == null ? logicalTable : t.get("task_name"));
                row.put("sync_mode", t.get("sync_mode"));
                row.put("full_scope", t.get("full_scope"));
                row.put("cron_expr", t.get("cron_expr"));
                row.put("start_mode", t.get("start_mode"));
                row.put("run_status", t.get("run_status"));
                row.put("status", t.get("run_status"));
                row.put("last_run_time", t.get("last_run_time"));
                row.put("multi_source", ((Number) t.get("cdc_count")).intValue() > 1);
                List<Map<String, Object>> cdcs = metaJdbc.queryForList(
                        "SELECT source_db, source_table, running FROM cdc_st_task WHERE logical_table = ? ORDER BY task_id",
                        logicalTable);
                List<String> sources = new ArrayList<>();
                boolean anyRunning = false;
                for (Map<String, Object> c : cdcs) {
                    sources.add(c.get("source_db") + "." + c.get("source_table"));
                    if (Integer.valueOf(1).equals(((Number) c.get("running")).intValue())) {
                        anyRunning = true;
                    }
                }
                row.put("sources", sources);
                row.put("running", anyRunning);
                out.add(row);
            }
        } catch (Exception e) {
            return ApiResponse.error(500, "任务列表查询失败: " + e.getMessage());
        }
        return ApiResponse.ok(out);
    }

    @PostMapping("/validate")
    @PreAuthorize("@perm.has('sync:task:write')")
    public ApiResponse<Map<String, Object>> validate(@RequestBody Map<String, Object> body) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fieldMapping = (List<Map<String, Object>>) body.get("fieldMapping");
        @SuppressWarnings("unchecked")
        List<String> available = (List<String>) body.get("availableFields");
        List<String> issues = service.validate(fieldMapping, available);
        return ApiResponse.ok(Map.of("valid", issues.isEmpty(), "issues", issues));
    }

    @PostMapping
    @PreAuthorize("@perm.has('sync:task:write')")
    public ApiResponse<Map<String, Object>> save(@RequestBody Map<String, Object> body) {
        String name = (String) body.getOrDefault("name", "同步任务");
        String logicalTable = (String) body.get("logicalTable");
        Long dsId = ((Number) body.getOrDefault("datasourceId", 0)).longValue();
        String sourceDb = (String) body.get("sourceDb");
        String sourceTable = (String) body.get("sourceTable");
        // 多表合并: sourceTables 数组(兼容单值 sourceTable)
        @SuppressWarnings("unchecked")
        List<String> sourceTables = (List<String>) body.get("sourceTables");
        if (sourceTables == null || sourceTables.isEmpty()) {
            sourceTables = sourceTable == null ? List.of() : List.of(sourceTable);
        }
        // 多数据源/多库: sources = [{datasourceId, sourceDb, sourceTables:[...]}] → 每源表各自的数据源/库
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sources = (List<Map<String, Object>>) body.get("sources");
        Map<String, Map<String, Object>> originByTable = null;
        if (sources != null && !sources.isEmpty()) {
            originByTable = new java.util.LinkedHashMap<>();
            List<String> union = new ArrayList<>();
            boolean first = true;
            for (Map<String, Object> s : sources) {
                Object sd = s.get("datasourceId");
                Object sdb = s.get("sourceDb");
                Object sts = s.get("sourceTables");
                if (sd == null || sdb == null || !(sts instanceof List<?> tl)) {
                    continue;
                }
                for (Object o : tl) {
                    String t = String.valueOf(o);
                    Map<String, Object> oo = new java.util.LinkedHashMap<>();
                    oo.put("datasourceId", ((Number) sd).longValue());
                    oo.put("sourceDb", String.valueOf(sdb));
                    originByTable.put(t, oo);
                    union.add(t);
                }
                if (first) {
                    dsId = ((Number) sd).longValue();
                    sourceDb = String.valueOf(sdb);
                    first = false;
                }
            }
            if (!union.isEmpty()) {
                sourceTables = union;
            }
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fieldMapping = (List<Map<String, Object>>) body.get("fieldMapping");
        // 提交前校验, 有问题则拒绝
        List<String> issues = service.validate(fieldMapping, null);
        if (!issues.isEmpty()) {
            return ApiResponse.ok(Map.of("saved", false, "issues", issues));
        }
        // 多表合并约束: 业务主键(mode=merge)禁止物理身份/表名进键
        @SuppressWarnings("unchecked")
        Map<String, Object> businessKey = (Map<String, Object>) body.get("businessKey");
        List<String> mergeIssues = service.validateMergeKey(businessKey);
        if (!mergeIssues.isEmpty()) {
            return ApiResponse.ok(Map.of("saved", false, "issues", mergeIssues));
        }
        boolean newInstance = Boolean.TRUE.equals(body.get("asNewInstance"));
        Long id = service.save(name, logicalTable, dsId, sourceDb, sourceTables,
                fieldMapping, body.get("rowChain"), businessKey,
                (String) body.get("syncMode"), (String) body.get("fullScope"),
                (String) body.get("cronExpr"), (String) body.get("startMode"),
                body.get("startValue") == null ? null : String.valueOf(body.get("startValue")),
                originByTable, newInstance);
        return ApiResponse.ok(Map.of("saved", true, "id", id, "newInstance", newInstance,
                "sources", sourceTables, "merge", sourceTables.size() > 1));
    }

    /** 实现 3: 提交 SeaTunnel CDC 直连任务(源表 → 目标表, 复用现有 seatunnel 环境) */
    @PostMapping("/seatunnel-cdc/start")
    @PreAuthorize("@perm.has('sync:task:run')")
    public ApiResponse<Map<String, Object>> seatunnelCdc(@RequestBody Map<String, Object> body) {
        Long srcDsId = ((Number) body.get("sourceDatasourceId")).longValue();
        String sourceDb = (String) body.get("sourceDb");
        String sourceTable = (String) body.get("sourceTable");
        Long tgtDsId = ((Number) body.getOrDefault("targetDatasourceId", srcDsId)).longValue();
        String targetTable = (String) body.get("targetTable");
        String primaryKey = (String) body.getOrDefault("primaryKey", "id");
        return ApiResponse.ok(seaTunnelCdcService.startCdc(srcDsId, sourceDb, sourceTable,
                tgtDsId, targetTable, primaryKey));
    }
}

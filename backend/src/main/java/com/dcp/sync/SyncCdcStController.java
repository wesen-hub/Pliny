package com.dcp.sync;

import com.dcp.common.ApiResponse;

import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * SeaTunnel CDC → Kafka 采集 API(架构文档采集端实现):
 *
 * <pre>
 *   POST /api/sync/cdc-st/task   注册采集任务 {logicalTable, datasourceId, sourceDb, sourceTable,
 *                                          businessKeyFields, keyColumns, topic?}
 *   POST /api/sync/cdc-st/start/{logicalTable}  启动(connector-cdc-mysql 全量+增量 → CdcSeqTransform → Kafka)
 *   POST /api/sync/cdc-st/stop/{logicalTable}   停止(checkpoint 落盘, 重启同 jobId 续采)
 *   POST /api/sync/cdc-st/alert-ddl             源表 DDL(TRUNCATE/DROP/ALTER)告警登记(N6)
 *   GET  /api/sync/cdc-st/status                全部任务 + SeaTunnel 实时状态
 * </pre>
 */
@RestController
@RequestMapping("/api/sync/cdc-st")
public class SyncCdcStController {

    private final CdcStService service;

    public SyncCdcStController(CdcStService service) {
        this.service = service;
    }

    @PostMapping("/task")
    public ApiResponse<Map<String, Object>> register(@RequestBody Map<String, Object> body) {
        String logicalTable = (String) body.get("logicalTable");
        Long dsId = ((Number) body.get("datasourceId")).longValue();
        String db = (String) body.get("sourceDb");
        String table = (String) body.get("sourceTable");
        @SuppressWarnings("unchecked")
        List<String> bk = (List<String>) body.get("businessKeyFields");
        @SuppressWarnings("unchecked")
        List<String> kc = (List<String>) body.get("keyColumns");
        String topic = (String) body.get("topic");
        String fieldMapping = body.get("fieldMapping") == null ? null
                : (body.get("fieldMapping") instanceof String s ? s
                : toJson(body.get("fieldMapping")));
        String rowChain = body.get("rowChain") == null ? null
                : (body.get("rowChain") instanceof String s ? s
                : toJson(body.get("rowChain")));
        long syncTaskId = body.get("syncTaskId") == null ? 0L
                : ((Number) body.get("syncTaskId")).longValue();
        service.register(logicalTable, dsId, db, table, bk, kc, topic, fieldMapping, rowChain, syncTaskId);
        return ApiResponse.ok(Map.of("registered", true, "logicalTable", logicalTable, "syncTaskId", syncTaskId));
    }

    private static String toJson(Object o) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @PostMapping("/start/{logicalTable}")
    public ApiResponse<Map<String, Object>> start(@PathVariable String logicalTable) {
        return ApiResponse.ok(service.start(logicalTable));
    }

    @PostMapping("/stop/{logicalTable}")
    public ApiResponse<Map<String, Object>> stop(@PathVariable String logicalTable) {
        return ApiResponse.ok(service.stop(logicalTable));
    }

    /** 实例级控制: 同(逻辑,源)多任务实例并存时精确启/停某一个 */
    @PostMapping("/start-instance/{taskId}")
    public ApiResponse<Map<String, Object>> startInstance(@PathVariable Long taskId) {
        return ApiResponse.ok(service.startInstance(taskId));
    }

    @PostMapping("/stop-instance/{taskId}")
    public ApiResponse<Map<String, Object>> stopInstance(@PathVariable Long taskId) {
        return ApiResponse.ok(service.stopInstance(taskId));
    }

    @GetMapping("/status")
    public ApiResponse<List<Map<String, Object>>> status() {
        return ApiResponse.ok(service.status());
    }

    /** N6(D1/D2): 源表 DDL(TRUNCATE/DROP/ALTER)告警登记——CDC 不输出 DDL 事件, 由 DBA 执行后登记 */
    @PostMapping("/alert-ddl")
    public ApiResponse<Map<String, Object>> alertDdl(@RequestBody Map<String, Object> body) {
        String logicalTable = (String) body.get("logicalTable");
        String ddlKind = (String) body.get("ddlKind");
        String detail = (String) body.get("detail");
        if (logicalTable == null || logicalTable.isBlank()) {
            throw new IllegalArgumentException("logicalTable 不能为空");
        }
        service.alertDdlChange(logicalTable, ddlKind, detail);
        return ApiResponse.ok(Map.of("alerted", true, "logicalTable", logicalTable));
    }
}

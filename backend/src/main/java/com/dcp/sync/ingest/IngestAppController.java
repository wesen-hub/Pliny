package com.dcp.sync.ingest;

import org.springframework.security.access.prepost.PreAuthorize;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Kafka 接入应用(接入任务)管理 API — 应用 = 页面领取的 appKey, op 由消息包内指定:
 *
 * <pre>
 *   GET    /api/ingest/apps                      应用列表(含绑定表/ok/死信)
 *   POST   /api/ingest/apps                      新建应用 → 颁发 appKey(只显示一次)
 *                                                {appName, description?, logicalTables:[...]}
 *   GET    /api/ingest/apps/{appKey}             应用详情: 分表 ok/死信/lag + 60s 吞吐
 *   POST   /api/ingest/apps/{appKey}/enable|disable   启停(停用=禁提交; 无启用应用的表自动停消费)
 *   POST   /api/ingest/apps/{appKey}/bind        重绑白名单 {logicalTables:[...]}
 *   DELETE /api/ingest/apps/{appKey}             删除
 * </pre>
 */
@RestController
@RequestMapping("/api/ingest/apps")
public class IngestAppController {

    private final KafkaIngestEngine engine;

    public IngestAppController(KafkaIngestEngine engine) {
        this.engine = engine;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(engine.listApps());
    }

    @PostMapping
    @PreAuthorize("@perm.has('ingest:app:manage')")
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            @SuppressWarnings("unchecked")
            List<String> tables = (List<String>) body.get("logicalTables");
            return ApiResponse.ok(engine.createApp((String) body.get("appName"),
                    (String) body.get("description"), tables));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @GetMapping("/{appKey}")
    public ApiResponse<Map<String, Object>> status(@PathVariable String appKey) {
        try {
            return ApiResponse.ok(engine.appStatus(appKey));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{appKey}/enable")
    @PreAuthorize("@perm.has('ingest:app:manage')")
    public ApiResponse<Map<String, Object>> enable(@PathVariable String appKey) {
        try {
            engine.enableApp(appKey, true);
            return ApiResponse.ok(Map.of("enabled", true, "appKey", appKey));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{appKey}/disable")
    @PreAuthorize("@perm.has('ingest:app:manage')")
    public ApiResponse<Map<String, Object>> disable(@PathVariable String appKey) {
        try {
            engine.enableApp(appKey, false);
            return ApiResponse.ok(Map.of("disabled", true, "appKey", appKey));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{appKey}/bind")
    @PreAuthorize("@perm.has('ingest:app:manage')")
    public ApiResponse<Map<String, Object>> bind(@PathVariable String appKey,
                                                 @RequestBody Map<String, Object> body) {
        try {
            @SuppressWarnings("unchecked")
            List<String> tables = (List<String>) body.get("logicalTables");
            engine.bindTables(appKey, tables == null ? List.of() : tables);
            return ApiResponse.ok(Map.of("bound", true, "appKey", appKey, "logicalTables", tables));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @DeleteMapping("/{appKey}")
    @PreAuthorize("@perm.has('ingest:app:manage')")
    public ApiResponse<Map<String, Object>> delete(@PathVariable String appKey) {
        engine.deleteApp(appKey);
        return ApiResponse.ok(Map.of("deleted", true, "appKey", appKey));
    }
}

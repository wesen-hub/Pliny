package com.dcp.module.datasource;

import com.dcp.common.ApiResponse;
import com.dcp.module.datasource.entity.DatasourceConfig;
import lombok.Data;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/datasources")
public class DatasourceController {

    private final DatasourceService service;

    public DatasourceController(DatasourceService service) {
        this.service = service;
    }

    @Data
    public static class DatasourceRequest {
        private Long id;
        private String name;
        private String type;
        private String host;
        private Integer port;
        private String databaseName;
        private String username;
        private String password;
        private String extraParams;
        private String groupName;
        private String env;
        private Integer enabled;
    }

    private DatasourceConfig toEntity(DatasourceRequest req) {
        DatasourceConfig ds = new DatasourceConfig();
        ds.setId(req.getId());
        ds.setName(req.getName());
        ds.setType(req.getType());
        ds.setHost(req.getHost());
        ds.setPort(req.getPort());
        ds.setDatabaseName(req.getDatabaseName());
        ds.setUsername(req.getUsername());
        ds.setExtraParams(req.getExtraParams());
        ds.setGroupName(req.getGroupName());
        ds.setEnv(req.getEnv());
        ds.setEnabled(req.getEnabled());
        return ds;
    }

    @GetMapping
    public ApiResponse<Map<String, Object>> page(@RequestParam(defaultValue = "1") int page,
                                                 @RequestParam(defaultValue = "10") int pageSize,
                                                 @RequestParam(required = false) String type,
                                                 @RequestParam(required = false) String group,
                                                 @RequestParam(required = false) String env,
                                                 @RequestParam(required = false) String keyword) {
        var p = service.page(page, pageSize, type, group, env, keyword);
        return ApiResponse.ok(Map.of("total", p.getTotal(), "list", p.getRecords()));
    }

    @GetMapping("/{id}")
    public ApiResponse<DatasourceConfig> get(@PathVariable Long id) {
        return ApiResponse.ok(service.get(id));
    }

    @PostMapping
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Long> create(@RequestBody DatasourceRequest req) {
        return ApiResponse.ok(service.create(toEntity(req), req.getPassword()));
    }

    @PutMapping("/{id}")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> update(@PathVariable Long id, @RequestBody DatasourceRequest req) {
        service.update(id, toEntity(req), req.getPassword());
        return ApiResponse.ok();
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        service.delete(id);
        return ApiResponse.ok();
    }

    @PostMapping("/{id}/enable")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> enable(@PathVariable Long id) {
        service.setEnabled(id, true);
        return ApiResponse.ok();
    }

    @PostMapping("/{id}/disable")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> disable(@PathVariable Long id) {
        service.setEnabled(id, false);
        return ApiResponse.ok();
    }

    @PostMapping("/{id}/test")
    public ApiResponse<Void> test(@PathVariable Long id) {
        service.testConnection(id);
        return ApiResponse.ok();
    }

    @GetMapping("/{id}/tables")
    public ApiResponse<List<Map<String, String>>> tables(@PathVariable Long id) {
        return ApiResponse.ok(service.listTables(id));
    }

    // ---------- V3 库/Schema/表 级联浏览(按数据库类型分层) ----------

    @GetMapping("/{id}/databases")
    public ApiResponse<List<String>> databases(@PathVariable Long id) {
        return ApiResponse.ok(service.listDatabases(id));
    }

    @GetMapping("/{id}/databases/{database}/schemas")
    public ApiResponse<List<String>> schemas(@PathVariable Long id, @PathVariable String database) {
        return ApiResponse.ok(service.listSchemas(id, database));
    }

    @GetMapping("/{id}/databases/{database}/tables")
    public ApiResponse<List<Map<String, String>>> dbTables(@PathVariable Long id,
                                                           @PathVariable String database,
                                                           @RequestParam(required = false) String schema) {
        return ApiResponse.ok(service.listTables(id, database, schema));
    }

    @GetMapping("/{id}/databases/{database}/tables/{table}/columns")
    public ApiResponse<List<Map<String, Object>>> dbColumns(@PathVariable Long id,
                                                            @PathVariable String database,
                                                            @PathVariable String table,
                                                            @RequestParam(required = false) String schema) {
        return ApiResponse.ok(service.listColumns(id, database, table, schema));
    }

    @GetMapping("/{id}/tables/{table}/columns")
    public ApiResponse<List<Map<String, Object>>> columns(@PathVariable Long id, @PathVariable String table) {
        return ApiResponse.ok(service.listColumns(id, table));
    }

    @PostMapping("/{id}/preview")
    public ApiResponse<Map<String, Object>> preview(@PathVariable Long id,
                                                    @RequestBody Map<String, Object> body) {
        String sql = (String) body.get("sql");
        Integer limit = body.get("limit") == null ? 100 : ((Number) body.get("limit")).intValue();
        return ApiResponse.ok(service.preview(id, sql, limit));
    }
}

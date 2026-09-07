package com.dcp.sync;

import com.dcp.common.ApiResponse;

import lombok.Data;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 逻辑表配置 API(P1): 业务主键/分表规则/唯一键列 的查看与保存。
 *
 * <pre>
 *   GET /api/sync/logical-tables           全部逻辑表配置
 *   GET /api/sync/logical-tables/{table}   单表配置
 *   PUT /api/sync/logical-tables/{table}   保存配置(键构成上线后不可变; 保存后自动注册消费监听)
 * </pre>
 *
 * 前端「业务主键配置」步骤提交时调用 PUT, 与 TaskWizard.businessKey 结构对应。
 */
@RestController
@RequestMapping("/api/sync/logical-tables")
public class LogicalTableController {

    private final LogicalTableConfigService service;
    private final JdbcTemplate metaJdbc;

    public LogicalTableController(LogicalTableConfigService service, JdbcTemplate metaJdbc) {
        this.service = service;
        this.metaJdbc = metaJdbc;
    }

    @Data
    public static class LogicalTableReq {
        private String topic;
        private Integer bucketCount = 4096;
        private Integer tableCount = 50;
        private String tablePrefix;
        private Integer partitionCount = 8;
        private String consumerGroup;
        /** {mode: merge|scope, scopeFields:[], systemNames:[], keyFields:[]} */
        private Map<String, Object> businessKey;
        private List<String> keyColumns;
        private String syncMode;
        private String fullScope;
        private String cronExpr;
        private String startMode;
        private String startValue;
    }

    @GetMapping
    public ApiResponse<List<LogicalTableConfigService.LogicalTableConfig>> list() {
        List<LogicalTableConfigService.LogicalTableConfig> out = new ArrayList<>();
        try {
            List<String> tables = metaJdbc.queryForList(
                    "SELECT table_name FROM logical_table ORDER BY table_name", String.class);
            for (String t : tables) {
                out.add(service.load(t));
            }
        } catch (Exception e) {
            // 元库未就绪/无配置时返回空列表
        }
        return ApiResponse.ok(out);
    }

    @GetMapping("/{table}")
    public ApiResponse<LogicalTableConfigService.LogicalTableConfig> get(@PathVariable String table) {
        return ApiResponse.ok(service.load(table));
    }

    @PutMapping("/{table}")
    public ApiResponse<Map<String, Object>> save(@PathVariable String table, @RequestBody LogicalTableReq req) {
        // 键构成上线不可变(多表合并: 同一逻辑表所有任务键必须一致; 变更 = 实体分裂 = 全量重迁)
        LogicalTableConfigService.LogicalTableConfig existing = service.load(table);
        if (existing.getKeyColumns() != null && !existing.getKeyColumns().isEmpty()
                && req.getKeyColumns() != null && !req.getKeyColumns().isEmpty()
                && !existing.getKeyColumns().equals(req.getKeyColumns())) {
            return ApiResponse.error(409, "业务主键上线后不可变(变更 = 实体分裂 = 全量重迁);"
                    + " 多表合并场景同一逻辑表所有任务键必须一致, 请新建逻辑表");
        }
        LogicalTableConfigService.LogicalTableConfig cfg = new LogicalTableConfigService.LogicalTableConfig();
        cfg.setTableName(table);
        cfg.setTopic(req.getTopic() == null ? "dcp." + table : req.getTopic());
        cfg.setBucketCount(req.getBucketCount() == null ? 4096 : req.getBucketCount());
        cfg.setTableCount(req.getTableCount() == null ? 50 : req.getTableCount());
        cfg.setTablePrefix(req.getTablePrefix());
        cfg.setPartitionCount(req.getPartitionCount() == null ? 8 : req.getPartitionCount());
        cfg.setConsumerGroup(req.getConsumerGroup());
        cfg.setBusinessKey(req.getBusinessKey());
        cfg.setKeyColumns(req.getKeyColumns());
        cfg.setSyncMode(req.getSyncMode());
        cfg.setFullScope(req.getFullScope());
        cfg.setCronExpr(req.getCronExpr());
        cfg.setStartMode(req.getStartMode());
        cfg.setStartValue(req.getStartValue());
        service.save(cfg);
        return ApiResponse.ok(Map.of("table", table, "saved", true));
    }
}

package com.dcp.module.job;

import com.dcp.common.ApiResponse;
import com.dcp.module.job.entity.ExtractConfig;
import com.dcp.module.job.entity.FieldMapping;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.entity.SyncState;
import com.dcp.module.job.service.JobService;
import lombok.Data;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    @Data
    public static class JobRequest {
        private Long id;
        private String jobName;
        private Long sourceDatasourceId;
        private Long targetDatasourceId;
        private String targetTableName;
        private String syncMode;
        private String incrementalField;
        private String cronExpr;
        private String misfirePolicy;
        private Integer enabled;
        private Integer timeoutMinutes;
        private Integer parallelism;
        private Integer retryTimes;
        private Integer retryIntervalSeconds;
        private Integer alarmEnabled;
        private String description;
        private ExtractConfig extractConfig;
        private List<FieldMapping> fieldMappings;
    }

    private JobDefinition toJob(JobRequest req) {
        JobDefinition job = new JobDefinition();
        job.setId(req.getId());
        job.setJobName(req.getJobName());
        job.setSourceDatasourceId(req.getSourceDatasourceId());
        job.setTargetDatasourceId(req.getTargetDatasourceId());
        job.setTargetTableName(req.getTargetTableName());
        job.setSyncMode(req.getSyncMode());
        job.setIncrementalField(req.getIncrementalField());
        job.setCronExpr(req.getCronExpr());
        job.setMisfirePolicy(req.getMisfirePolicy() == null ? "DO_NOTHING" : req.getMisfirePolicy());
        job.setEnabled(req.getEnabled() == null ? 1 : req.getEnabled());
        job.setTimeoutMinutes(req.getTimeoutMinutes() == null ? 120 : req.getTimeoutMinutes());
        job.setParallelism(req.getParallelism() == null ? 2 : req.getParallelism());
        job.setRetryTimes(req.getRetryTimes() == null ? 3 : req.getRetryTimes());
        job.setRetryIntervalSeconds(req.getRetryIntervalSeconds() == null ? 60 : req.getRetryIntervalSeconds());
        job.setAlarmEnabled(req.getAlarmEnabled() == null ? 1 : req.getAlarmEnabled());
        job.setDescription(req.getDescription());
        return job;
    }

    @GetMapping
    public ApiResponse<Map<String, Object>> page(@RequestParam(defaultValue = "1") int page,
                                                 @RequestParam(defaultValue = "10") int pageSize,
                                                 @RequestParam(required = false) String keyword,
                                                 @RequestParam(required = false) String status,
                                                 @RequestParam(required = false) String syncMode) {
        var p = service.page(page, pageSize, keyword, status, syncMode);
        return ApiResponse.ok(Map.of("total", p.getTotal(), "list", p.getRecords()));
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> detail(@PathVariable Long id) {
        return ApiResponse.ok(service.detail(id));
    }

    @PostMapping
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Long> create(@RequestBody JobRequest req) {
        return ApiResponse.ok(service.create(toJob(req), req.getExtractConfig(), req.getFieldMappings()));
    }

    @PutMapping("/{id}")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> update(@PathVariable Long id, @RequestBody JobRequest req) {
        req.setId(id);
        service.update(id, toJob(req), req.getExtractConfig(), req.getFieldMappings());
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

    @PostMapping("/{id}/run")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Map<String, Object>> run(@PathVariable Long id) {
        Long executionId = service.run(id, "MANUAL");
        return ApiResponse.ok(Map.of("executionId", executionId));
    }

    @PostMapping("/{id}/stop")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> stop(@PathVariable Long id) {
        service.stop(id);
        return ApiResponse.ok();
    }

    @PostMapping("/{id}/copy")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Long> copy(@PathVariable Long id) {
        return ApiResponse.ok(service.copy(id));
    }

    @GetMapping("/{id}/sync-state")
    public ApiResponse<SyncState> syncState(@PathVariable Long id) {
        return ApiResponse.ok(service.syncState(id));
    }

    @PutMapping("/{id}/sync-state")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> resetSyncState(@PathVariable Long id,
                                            @RequestBody Map<String, String> body) {
        service.resetSyncState(id, body.get("value"), body.get("valueType"));
        return ApiResponse.ok();
    }

    @PostMapping("/mappings/expression/validate")
    public ApiResponse<Map<String, Object>> validateExpression(@RequestBody Map<String, Object> body) {
        String expression = (String) body.get("expression");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) body.get("availableFields");
        return ApiResponse.ok(service.validateExpression(expression, fields));
    }

    @GetMapping("/cron/preview")
    public ApiResponse<List<String>> cronPreview(@RequestParam String expr) {
        return ApiResponse.ok(service.cronPreview(expr));
    }

    @PostMapping("/mappings/auto")
    public ApiResponse<List<Map<String, Object>>> autoMapping(@RequestBody Map<String, Object> body) {
        @SuppressWarnings("unchecked")
        List<String> source = (List<String>) body.get("sourceColumns");
        @SuppressWarnings("unchecked")
        List<String> target = (List<String>) body.get("targetColumns");
        return ApiResponse.ok(service.autoMapping(source, target));
    }
}

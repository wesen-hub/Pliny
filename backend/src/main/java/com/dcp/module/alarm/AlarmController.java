package com.dcp.module.alarm;

import com.dcp.common.ApiResponse;
import com.dcp.module.alarm.entity.AlarmConfig;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/alarms")
public class AlarmController {

    private final AlarmService service;

    public AlarmController(AlarmService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<List<AlarmConfig>> list(@RequestParam(required = false) Long jobId) {
        return ApiResponse.ok(service.list(jobId));
    }

    @PostMapping
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Long> create(@RequestBody AlarmConfig cfg) {
        return ApiResponse.ok(service.create(cfg));
    }

    @PutMapping("/{id}")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> update(@PathVariable Long id, @RequestBody AlarmConfig cfg) {
        service.update(id, cfg);
        return ApiResponse.ok();
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.canWrite()")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        service.delete(id);
        return ApiResponse.ok();
    }
}

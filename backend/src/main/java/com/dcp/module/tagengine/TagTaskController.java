package com.dcp.module.tagengine;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 打标引擎 V2 —— 打标任务 API(P0 仅批):
 *
 * <pre>
 *   GET    /api/tagengine/tasks               列表
 *   POST   /api/tagengine/tasks               新建(bindings 按执行序: 规则先跑 → 规则流后跑)
 *   GET    /api/tagengine/tasks/{id}          详情
 *   PUT    /api/tagengine/tasks/{id}          编辑
 *   DELETE /api/tagengine/tasks/{id}          删除
 *   POST   /api/tagengine/tasks/{id}/publish  发布(固化规则版本快照; DRL/字段编译校验后续刀接入)
 * </pre>
 */
@RestController
@RequestMapping("/api/tagengine/tasks")
public class TagTaskController {

    private final TagEngineService service;

    public TagTaskController(TagEngineService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(service.listTasks());
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> get(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.getTask(id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.createTask(body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PutMapping("/{id}")
    public ApiResponse<Map<String, Object>> update(@PathVariable long id, @RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.updateTask(id, body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Map<String, Object>> delete(@PathVariable long id) {
        try {
            service.deleteTask(id);
            return ApiResponse.ok(Map.of("deleted", true, "id", id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/publish")
    public ApiResponse<Map<String, Object>> publish(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.publishTask(id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    /** 手动触发一次批运行(P0 演练/提交器; cron 由服务端 tick 自动触发) */
    @PostMapping("/{id}/run")
    public ApiResponse<Map<String, Object>> run(@PathVariable long id,
                                                @RequestParam(required = false) String trigger,
                                                @RequestParam(required = false, defaultValue = "false") boolean full) {
        try {
            return ApiResponse.ok(service.startRun(id, trigger == null ? "manual" : trigger, full));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}

package com.dcp.module.datasub;

import com.dcp.common.ApiResponse;
import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 数据订阅 API(2026-09-09 方案):
 * <pre>
 * 管理(平台/数据管理员): POST /api/data-sub(申请) GET(列表) GET/{id} POST/{id}/approve|reject|stop POST/{id}/deliver
 * 订阅方 PULL(数据服务 API): GET /{id}/schema | /{id}/snapshot | /{id}/changes?after | /{id}/export?format=csv
 *   —— 鉴权: X-Dcp-Token(审批生效签发); 未生效/到期=403
 * </pre>
 */
@RestController
@RequestMapping("/api/data-sub")
@RequiredArgsConstructor
public class DataSubController {

    private final DataSubService service;

    private static String s(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    /* ---------- 管理面 ---------- */

    @GetMapping("/catalog")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> catalog() {
        return ApiResponse.ok(service.catalog());
    }

    @GetMapping
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(service.list());
    }

    @GetMapping("/{id}")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> get(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.getContract(id));
        } catch (BizException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.create(body, SecurityUtils.currentUsername()));
        } catch (BizException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/approve")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> approve(@PathVariable long id, @RequestBody(required = false) Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.approve(id, body == null ? Map.of() : body, SecurityUtils.currentUsername()));
        } catch (BizException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/reject")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> reject(@PathVariable long id, @RequestBody(required = false) Map<String, Object> body) {
        try {
            service.reject(id, s(body == null ? null : body.get("reason")), SecurityUtils.currentUsername());
            return ApiResponse.ok(Map.of("rejected", true, "id", id));
        } catch (BizException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/stop")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> stop(@PathVariable long id) {
        try {
            service.stop(id);
            return ApiResponse.ok(Map.of("stopped", true, "id", id));
        } catch (BizException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    /** PUSH 投递: 默认全量; ?incremental=true → 按契约 last_cursor 增量(调度/重试用) */
    @PostMapping("/{id}/deliver")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> deliver(@PathVariable long id,
                                                    @RequestParam(defaultValue = "false") boolean incremental) {
        try {
            return ApiResponse.ok(service.deliver(id, SecurityUtils.currentUsername(), incremental));
        } catch (BizException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    /** 交付/拉取记录(管理查看) */
    @GetMapping("/{id}/deliveries")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<List<Map<String, Object>>> deliveries(@PathVariable long id) {
        return ApiResponse.ok(service.deliveries(id));
    }

    /* ---------- 订阅方 PULL(数据服务 API; X-Dcp-Token 鉴权) ---------- */

    @GetMapping("/{id}/schema")
    public ApiResponse<Map<String, Object>> schema(@PathVariable long id,
                                                   @RequestHeader(value = "X-Dcp-Token", required = false) String token) {
        try {
            return ApiResponse.ok(service.schema(id, token));
        } catch (BizException e) {
            return ApiResponse.error(403, e.getMessage());
        }
    }

    @GetMapping("/{id}/snapshot")
    public ApiResponse<Map<String, Object>> snapshot(@PathVariable long id,
                                                     @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                     @RequestParam(defaultValue = "1000") int limit) {
        try {
            return ApiResponse.ok(service.pull(id, token, null, limit, false));
        } catch (BizException e) {
            return ApiResponse.error(403, e.getMessage());
        }
    }

    @GetMapping("/{id}/changes")
    public ApiResponse<Map<String, Object>> changes(@PathVariable long id,
                                                    @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                    @RequestParam(required = false) String after,
                                                    @RequestParam(defaultValue = "1000") int limit) {
        try {
            return ApiResponse.ok(service.pull(id, token, after, limit, true));
        } catch (BizException e) {
            return ApiResponse.error(403, e.getMessage());
        }
    }

    /* ---------- 异步导出(方案 3.1: 创建任务 → 轮询 → TTL 内下载) ---------- */

    @PostMapping("/{id}/export")
    public ApiResponse<Map<String, Object>> createExport(@PathVariable long id,
                                                         @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                         @RequestParam(defaultValue = "csv") String format) {
        if (!"csv".equals(format) && !"json".equals(format)) throw new BizException(ErrorCode.PARAM_ERROR, "format=csv|json");
        try {
            return ApiResponse.ok(service.createExport(id, token, SecurityUtils.hasRole("ADMIN"), format));
        } catch (BizException e) {
            return ApiResponse.error(403, e.getMessage());
        }
    }

    @GetMapping("/export-task/{taskId}")
    public ApiResponse<Map<String, Object>> exportStatus(@PathVariable long taskId,
                                                         @RequestHeader(value = "X-Dcp-Token", required = false) String token) {
        try {
            return ApiResponse.ok(service.exportStatus(taskId, token, SecurityUtils.hasRole("ADMIN")));
        } catch (BizException e) {
            return ApiResponse.error(e.getCode() == ErrorCode.NOT_FOUND.getCode() ? 404 : 403, e.getMessage());
        }
    }

    @GetMapping(value = "/export-task/{taskId}/download", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public byte[] exportDownload(@PathVariable long taskId,
                                 @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                 jakarta.servlet.http.HttpServletResponse response) {
        byte[] data = service.exportDownload(taskId, token, SecurityUtils.hasRole("ADMIN"));
        response.setHeader("Content-Disposition", "attachment; filename=dcp-export-" + taskId + ".dat");
        return data;
    }
}

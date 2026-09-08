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

    /** PUSH 投递(平台触发; 后续接调度实现定时/实时) */
    @PostMapping("/{id}/deliver")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> deliver(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.deliver(id, SecurityUtils.currentUsername()));
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
                                                    @RequestParam(required = false) Long after,
                                                    @RequestParam(defaultValue = "1000") int limit) {
        try {
            return ApiResponse.ok(service.pull(id, token, after, limit, true));
        } catch (BizException e) {
            return ApiResponse.error(403, e.getMessage());
        }
    }

    /** 批量导出(异步文件语义: 小数据同步返回; 大数据走任务分片 —— 本期同步 CSV 输出) */
    @GetMapping(value = "/{id}/export", produces = MediaType.TEXT_PLAIN_VALUE)
    public String export(@PathVariable long id,
                         @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                         @RequestParam(defaultValue = "csv") String format) {
        if (!"csv".equals(format)) throw new BizException(ErrorCode.PARAM_ERROR, "本期支持 csv");
        return service.exportCsv(id, token);
    }
}

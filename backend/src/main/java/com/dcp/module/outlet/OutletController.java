package com.dcp.module.outlet;

import com.dcp.common.ApiResponse;
import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;
import com.dcp.module.outlet.source.MockChangeStreamSource;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 变更流出口 API(L3,设计 §6.9 六个接口)。
 *
 * <pre>
 * GET  /api/chg/{stream}/stream-status        → {epoch, earliest, latest, watermarkTs, retentionHours}
 * GET  /api/chg/{stream}/snapshot             → {boundary, rows, pageToken, done}       (快照段)
 * GET  /api/chg/{stream}/changes?after=&limit=→ {events, nextPosition, hasMore, watermarkTs} (增量段)
 * GET  /api/chg/{stream}/key/{bizKey}?asOf=   → 按主键回查(兜底,不用于常规消费)
 * GET  /api/chg/{stream}/position-at?time=    → 时间戳 → 位点
 * POST /api/chg/{stream}/commit?position=     → 上报位点(平台仅观测)
 *
 * 鉴权(骨架):请求头 X-Dcp-Token 或 query consumerId;生产改为契约 token 解析消费方身份(§7.5)。
 * 另附调试接口:POST /_mock/append(仅 mock 数据源)、GET /_debug/registry。
 * </pre>
 */
@RestController
@RequestMapping("/api/chg")
@RequiredArgsConstructor
public class OutletController {

    private final ChangeOutletService service;
    private final OutletRegistry registry;
    private final ObjectProvider<MockChangeStreamSource> mockSource;

    private String consumer(String token, String consumerId) {
        return (token != null && !token.isBlank()) ? token : consumerId;
    }

    @GetMapping("/{stream}/stream-status")
    public ApiResponse<StreamStatus> streamStatus(@PathVariable String stream,
                                                  @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                  @RequestParam(required = false) String consumerId) {
        return ApiResponse.ok(service.streamStatus(consumer(token, consumerId), stream));
    }

    @GetMapping("/{stream}/snapshot")
    public ApiResponse<SnapshotPage> snapshot(@PathVariable String stream,
                                              @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                              @RequestParam(required = false) String consumerId,
                                              @RequestParam(required = false) Long boundaryTso,
                                              @RequestParam(required = false) String pageToken,
                                              @RequestParam(defaultValue = "1000") int limit) {
        return ApiResponse.ok(service.snapshot(consumer(token, consumerId), stream, boundaryTso, pageToken, limit));
    }

    @GetMapping("/{stream}/changes")
    public ApiResponse<ChangeBatch> changes(@PathVariable String stream,
                                            @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                            @RequestParam(required = false) String consumerId,
                                            @RequestParam(required = false) String after,
                                            @RequestParam(defaultValue = "500") int limit) {
        return ApiResponse.ok(service.changes(consumer(token, consumerId), stream, after, limit));
    }

    @GetMapping("/{stream}/key/{bizKey}")
    public ApiResponse<Map<String, Object>> key(@PathVariable String stream,
                                                @PathVariable String bizKey,
                                                @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                @RequestParam(required = false) String consumerId,
                                                @RequestParam(required = false) Long asOf) {
        Map<String, Object> k = new LinkedHashMap<>();
        k.put("id", bizKey);
        return ApiResponse.ok(service.keyLookup(consumer(token, consumerId), stream, k, asOf));
    }

    @GetMapping("/{stream}/position-at")
    public ApiResponse<Map<String, Object>> positionAt(@PathVariable String stream,
                                                       @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                       @RequestParam(required = false) String consumerId,
                                                       @RequestParam long time) {
        return ApiResponse.ok(service.positionAt(consumer(token, consumerId), stream, time));
    }

    @PostMapping("/{stream}/commit")
    public ApiResponse<Map<String, Object>> commit(@PathVariable String stream,
                                                   @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                   @RequestParam(required = false) String consumerId,
                                                   @RequestParam String position) {
        return ApiResponse.ok(service.commit(consumer(token, consumerId), stream, position));
    }

    /* ---------------------------------------------------------- 调试接口(仅 mock 数据源) */

    /**
     * 模拟一次数据面变更(仅当 {@code dcp.outlet.source=mock} 时可用),
     * 用于在没有 TiDB 的环境里验证"写 → 事件 → 消费"链路。
     */
    @PostMapping("/{stream}/_mock/append")
    public ApiResponse<Map<String, Object>> mockAppend(@PathVariable String stream,
                                                       @RequestParam(defaultValue = "INSERT") String op,
                                                       @RequestBody Map<String, Object> row) {
        MockChangeStreamSource src = mockSource.getIfAvailable();
        if (src == null) {
            return ApiResponse.error(OutletErrorCode.OUTLET_INTERNAL, "当前数据源非 mock,不可模拟写入");
        }
        var e = src.append(stream, op.toUpperCase(), row, null);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tso", e.getTso());
        m.put("op", e.getOp());
        m.put("row", e.getRow());
        return ApiResponse.ok(m);
    }

    /** 查看已注册消费方/契约(调试) */
    @GetMapping("/_debug/registry")
    public ApiResponse<Map<String, Object>> registry() {
        return ApiResponse.ok(registry.snapshotForDebug());
    }

    /** 查看平台侧位点观测(调试) */
    @GetMapping("/{stream}/_debug/seen")
    public ApiResponse<Map<String, Object>> seen(@PathVariable String stream,
                                                 @RequestParam String consumerId) {
        return ApiResponse.ok(service.seen(consumerId, stream));
    }
}

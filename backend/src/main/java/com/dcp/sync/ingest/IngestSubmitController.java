package com.dcp.sync.ingest;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Kafka 接入提交 API(业务系统调用, 需携带 appKey):
 *
 * <pre>
 *   POST /api/ingest/{logicalTable}   提交消息(鉴权 + 白名单 + 预检)
 *        Header: Authorization: Bearer &lt;appKey&gt;(必带; 无效/停用→401; 未绑定该表→403)
 *        Body:   { "op": "I|U|D"(可选, 缺省 INSERT), "data": { 与逻辑表字段同名 } }
 *   GET  /api/ingest/meta/{logicalTable}   SDK bootstrap(需鉴权): 业务键字段 + 入站 topic
 * </pre>
 */
@RestController
@RequestMapping("/api/ingest")
public class IngestSubmitController {

    private final KafkaIngestEngine engine;

    public IngestSubmitController(KafkaIngestEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/{logicalTable}")
    public ApiResponse<Map<String, Object>> submit(@PathVariable String logicalTable,
                                                   @RequestHeader(value = "Authorization", required = false) String auth,
                                                   @RequestBody Map<String, Object> body) {
        String appKey = bearer(auth);
        if (appKey == null) {
            return ApiResponse.error(401, "缺少 appKey(Authorization: Bearer <appKey>)");
        }
        Object data = body.get("data");
        if (!(data instanceof Map)) {
            return ApiResponse.error(400, "data 必须为 JSON 对象(字段与逻辑表字段同名)");
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> dataMap = (Map<String, Object>) data;
            engine.submit(logicalTable, appKey, (String) body.get("op"),
                    body.get("ts") == null ? null : String.valueOf(body.get("ts")), dataMap);
            return ApiResponse.ok(Map.of("accepted", true, "logicalTable", logicalTable));
        } catch (KafkaIngestEngine.AuthException e) {
            return ApiResponse.error(401, e.getMessage());
        } catch (KafkaIngestEngine.ForbiddenException e) {
            return ApiResponse.error(403, e.getMessage());
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @GetMapping("/meta/{logicalTable}")
    public ApiResponse<Map<String, Object>> meta(@PathVariable String logicalTable,
                                                 @RequestHeader(value = "Authorization", required = false) String auth) {
        String appKey = bearer(auth);
        if (appKey == null) {
            return ApiResponse.error(401, "缺少 appKey(Authorization: Bearer <appKey>)");
        }
        try {
            return ApiResponse.ok(engine.meta(logicalTable, appKey));
        } catch (KafkaIngestEngine.AuthException e) {
            return ApiResponse.error(401, e.getMessage());
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    private static String bearer(String auth) {
        if (auth == null || auth.isBlank()) {
            return null;
        }
        String v = auth.trim();
        if (v.regionMatches(true, 0, "Bearer ", 0, 7)) {
            v = v.substring(7).trim();
        }
        return v.isEmpty() ? null : v;
    }
}

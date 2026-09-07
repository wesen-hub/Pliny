package com.dcp.module.agent;

import com.dcp.common.ApiResponse;
import com.dcp.sync.filesync.FileSyncTaskService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 采集 Agent 注册表与指令通道 API:
 *
 * <pre>
 *   平台管理(需登录):
 *     POST /api/agents                     创建 Agent 记录, 返回一次性注册码
 *     GET  /api/agents                     列表(在线状态/绑定任务数)
 *     DELETE /api/agents/{id}              删除(仅无绑定任务)
 *
 *   Agent 通道(secret 自校验, 无需 JWT; 见 SecurityConfig permitAll):
 *     POST /api/agents/register            注册码换 agentId+secret
 *     POST /api/agents/{id}/heartbeat      心跳(X-Agent-Secret)
 *     GET  /api/agents/{id}/commands       长轮询取指令?timeout=25
 *     POST /api/agents/{id}/commands/ack   {commandId}
 *     GET  /api/agents/{id}/tasks/expected 对账: 平台期望任务列表
 *     POST /api/agents/{id}/runs           执行结果上报(runId 幂等)
 * </pre>
 */
@RestController
@RequestMapping("/api/agents")
public class AgentController {

    private static final String HDR_SECRET = "X-Agent-Secret";

    private final AgentService service;
    private final FileSyncTaskService fileSyncService;

    public AgentController(AgentService service, FileSyncTaskService fileSyncService) {
        this.service = service;
        this.fileSyncService = fileSyncService;
    }

    // ───────────── 平台管理(登录态) ─────────────

    @PostMapping
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.createAgent(
                    (String) body.get("name"),
                    (String) body.get("host"),
                    (String) body.get("description")));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(service.listAgents());
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Map<String, Object>> delete(@PathVariable long id) {
        try {
            service.deleteAgent(id);
            return ApiResponse.ok(Map.of("deleted", true, "id", id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    // ───────────── Agent 通道(secret) ─────────────

    @PostMapping("/register")
    public ApiResponse<Map<String, Object>> register(@RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.register(
                    (String) body.get("code"),
                    (String) body.get("host"),
                    (String) body.get("os"),
                    (String) body.get("arch"),
                    body.get("version") == null ? null : String.valueOf(body.get("version"))));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/heartbeat")
    public ApiResponse<Map<String, Object>> heartbeat(@PathVariable long id,
                                                      @RequestHeader(value = HDR_SECRET, required = false) String secret,
                                                      @RequestBody(required = false) Map<String, Object> body) {
        if (!service.checkSecret(id, secret)) {
            return ApiResponse.error(401, "Agent secret 无效");
        }
        service.heartbeat(id, body);
        return ApiResponse.ok(Map.of("ok", true, "agentId", id, "now", System.currentTimeMillis()));
    }

    @GetMapping("/{id}/commands")
    public ApiResponse<Map<String, Object>> poll(@PathVariable long id,
                                                 @RequestHeader(value = HDR_SECRET, required = false) String secret,
                                                 @RequestParam(defaultValue = "25") int timeout) {
        if (!service.checkSecret(id, secret)) {
            return ApiResponse.error(401, "Agent secret 无效");
        }
        int t = Math.min(Math.max(timeout, 1), 60);
        return ApiResponse.ok(Map.of("commands", service.pollCommands(id, t)));
    }

    @PostMapping("/{id}/commands/ack")
    public ApiResponse<Map<String, Object>> ack(@PathVariable long id,
                                                @RequestHeader(value = HDR_SECRET, required = false) String secret,
                                                @RequestBody Map<String, Object> body) {
        if (!service.checkSecret(id, secret)) {
            return ApiResponse.error(401, "Agent secret 无效");
        }
        Number cid = (Number) body.get("commandId");
        if (cid == null) {
            return ApiResponse.error(400, "缺少 commandId");
        }
        service.ackCommand(id, cid.longValue());
        return ApiResponse.ok(Map.of("acked", true));
    }

    @GetMapping("/{id}/tasks/expected")
    public ApiResponse<List<Map<String, Object>>> expected(@PathVariable long id,
                                                           @RequestHeader(value = HDR_SECRET, required = false) String secret) {
        if (!service.checkSecret(id, secret)) {
            return ApiResponse.error(401, "Agent secret 无效");
        }
        return ApiResponse.ok(service.expectedTasks(id));
    }

    @PostMapping("/{id}/tasks/{taskId}/resend")
    public ApiResponse<Map<String, Object>> resend(@PathVariable long id, @PathVariable long taskId,
                                                   @RequestHeader(value = HDR_SECRET, required = false) String secret) {
        if (!service.checkSecret(id, secret)) {
            return ApiResponse.error(401, "Agent secret 无效");
        }
        boolean ok = fileSyncService.resendAgentTask(id, taskId);
        return ok ? ApiResponse.ok(Map.of("ok", true, "taskId", taskId))
                : ApiResponse.error(404, "任务不存在或不属于该 Agent: " + taskId);
    }

    @PostMapping("/{id}/runs")
    public ApiResponse<Map<String, Object>> report(@PathVariable long id,
                                                   @RequestHeader(value = HDR_SECRET, required = false) String secret,
                                                   @RequestBody Map<String, Object> body) {
        if (!service.checkSecret(id, secret)) {
            return ApiResponse.error(401, "Agent secret 无效");
        }
        try {
            return ApiResponse.ok(service.reportRun(id, body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}

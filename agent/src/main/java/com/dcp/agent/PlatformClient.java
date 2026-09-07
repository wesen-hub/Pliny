package com.dcp.agent;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 平台 API 客户端(全部走 secret 头自校验):
 * register / heartbeat / commands(长轮询) / ack / expected / runs
 */
public final class PlatformClient {

    public static class ApiException extends RuntimeException {
        public final int code;

        public ApiException(int code, String msg) {
            super(msg);
            this.code = code;
        }
    }

    private final AgentConfig cfg;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10)).build();

    public PlatformClient(AgentConfig cfg) {
        this.cfg = cfg;
    }

    private JsonNode call(String method, String path, String secret, Object body, Duration timeout) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(URI.create(cfg.platformUrl + path))
                    .timeout(timeout);
            if ("GET".equals(method)) {
                b.GET();
            } else {
                byte[] payload = body == null ? new byte[0]
                        : AgentConfig.OM.writeValueAsBytes(body);
                b.method(method, HttpRequest.BodyPublishers.ofByteArray(payload));
                b.header("Content-Type", "application/json");
            }
            if (secret != null) {
                b.header("X-Agent-Secret", secret);
            }
            HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
            JsonNode root = AgentConfig.OM.readTree(resp.body());
            int code = root.path("code").asInt(-1);
            if (code != 0) {
                throw new ApiException(code, root.path("message").asText("平台错误 code=" + code));
            }
            return root.path("data");
        } catch (ApiException e) {
            throw e;
        } catch (Exception e) {
            throw new ApiException(-1, "平台不可达(" + cfg.platformUrl + "): " + e.getMessage());
        }
    }

    public Map<String, Object> register(String code) {
        JsonNode d = call("POST", "/api/agents/register", null, Map.of(
                "code", code, "host", cfg.host, "os", cfg.os, "arch", cfg.arch, "version", cfg.version),
                Duration.ofSeconds(20));
        return Map.of("agentId", d.path("agentId").asLong(), "secret", d.path("secret").asText(),
                "name", d.path("name").asText(""));
    }

    public void heartbeat(long agentId, String secret) {
        call("POST", "/api/agents/" + agentId + "/heartbeat", secret,
                Map.of("version", cfg.version), Duration.ofSeconds(15));
    }

    /** 长轮询取指令; 网络失败抛 ApiException(调用方决定退避) */
    public List<Map<String, Object>> pollCommands(long agentId, String secret) {
        JsonNode d = call("GET", "/api/agents/" + agentId + "/commands?timeout=" + cfg.pollTimeoutSec, secret, null,
                Duration.ofSeconds(cfg.pollTimeoutSec + 15));
        return AgentConfig.OM.convertValue(d.path("commands"),
                new com.fasterxml.jackson.core.type.TypeReference<List<Map<String, Object>>>() {});
    }

    public void ack(long agentId, String secret, long commandId) {
        call("POST", "/api/agents/" + agentId + "/commands/ack", secret, Map.of("commandId", commandId),
                Duration.ofSeconds(15));
    }

    public List<Map<String, Object>> expectedTasks(long agentId, String secret) {
        JsonNode d = call("GET", "/api/agents/" + agentId + "/tasks/expected", secret, null, Duration.ofSeconds(20));
        return AgentConfig.OM.convertValue(d,
                new com.fasterxml.jackson.core.type.TypeReference<List<Map<String, Object>>>() {});
    }

    public void reportRun(long agentId, String secret, Map<String, Object> run) {
        call("POST", "/api/agents/" + agentId + "/runs", secret, run, Duration.ofSeconds(30));
    }

    /** 对账: 请求平台重发某任务的 task_upsert 指令(任务定义) */
    public void resendTask(long agentId, String secret, long taskId) {
        call("POST", "/api/agents/" + agentId + "/tasks/" + taskId + "/resend", secret, null, Duration.ofSeconds(20));
    }
}

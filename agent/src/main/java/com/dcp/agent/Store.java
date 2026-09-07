package com.dcp.agent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

/** 本地持久化(JSON 文件, 原子写): 凭据 / 任务定义 / 待上报结果队列 */
public final class Store {

    private final Path dir;
    private final Path credFile;
    private final Path taskFile;
    private final Path pendingFile;

    public Store(String dataDir) throws IOException {
        dir = Path.of(dataDir);
        Files.createDirectories(dir);
        credFile = dir.resolve("credentials.json");
        taskFile = dir.resolve("tasks.json");
        pendingFile = dir.resolve("pending-runs.json");
    }

    // ── 凭据 ──
    public synchronized Map<String, Object> credentials() {
        return readJson(credFile);
    }

    public synchronized void saveCredentials(long agentId, String secret) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("agentId", agentId);
        m.put("secret", secret);
        writeJson(credFile, m);
    }

    // ── 任务列表 ──
    public synchronized List<Map<String, Object>> tasks() {
        List<Map<String, Object>> l = readList(taskFile);
        return l == null ? new ArrayList<>() : l;
    }

    public synchronized void saveTasks(List<Map<String, Object>> tasks) {
        writeJson(taskFile, tasks);
    }

    // ── 待上报 run ──
    public synchronized List<Map<String, Object>> pendingRuns() {
        List<Map<String, Object>> l = readList(pendingFile);
        return l == null ? new ArrayList<>() : l;
    }

    public synchronized void savePendingRuns(List<Map<String, Object>> runs) {
        writeJson(pendingFile, runs);
    }

    // ── JSON IO ──
    private Map<String, Object> readJson(Path p) {
        try {
            if (!Files.exists(p)) return new LinkedHashMap<>();
            JsonNode n = AgentConfig.OM.readTree(Files.readAllBytes(p));
            if (n != null && n.isObject()) return AgentConfig.OM.convertValue(n, new TypeReference<LinkedHashMap<String, Object>>() {});
        } catch (Exception ignored) {
        }
        return new LinkedHashMap<>();
    }

    private List<Map<String, Object>> readList(Path p) {
        try {
            if (!Files.exists(p)) return new ArrayList<>();
            JsonNode n = AgentConfig.OM.readTree(Files.readAllBytes(p));
            if (n != null && n.isArray()) {
                return AgentConfig.OM.convertValue(n, new TypeReference<ArrayList<Map<String, Object>>>() {});
            }
            return new ArrayList<>();
        } catch (Exception e) {
            return null;
        }
    }

    private void writeJson(Path p, Object obj) {
        try {
            byte[] bytes = AgentConfig.OM.writeValueAsBytes(obj);
            Path tmp = p.resolveSibling(p.getFileName() + ".tmp");
            Files.write(tmp, bytes);
            Files.move(tmp, p, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("本地存储写入失败: " + p + " : " + e.getMessage(), e);
        }
    }
}

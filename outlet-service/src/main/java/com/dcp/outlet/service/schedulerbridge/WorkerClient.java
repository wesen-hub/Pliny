package com.dcp.outlet.service.schedulerbridge;

import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.model.TaskStatus;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * dcp-worker REST 客户端(方案 §9 提交链路 / C2 ②):outlet-service 不再直连 etcd,
 * TaskSpec 提交/注销/探针读状态统一走 dcp-worker。
 *
 * <pre>
 *   写: POST /tasks | DELETE /tasks/{ns}/{taskId} | PUT /tasks/{ns}/{taskId}/events/{name}
 *   读: GET /tasks/{ns} | GET /tasks/{ns}/{taskId}/spec | GET /tasks/{ns}/{taskId}/status
 *       GET /tasks/{ns}/{taskId}/events/{name} | GET /tasks/{ns}/{taskId}/snaps
 * </pre>
 *
 * worker 不可达时:写路径抛异常(上层回显),读路径返回空(不阻塞页面/巡检)。
 */
@Component
public class WorkerClient {

    private static final Logger log = LoggerFactory.getLogger(WorkerClient.class);

    private static final ObjectMapper OM = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final RestClient rest;

    public WorkerClient(@Value("${dcp.worker.url:http://localhost:8097}") String workerUrl) {
        this.rest = RestClient.builder()
                .baseUrl(workerUrl)
                .requestFactory(new JdkClientHttpRequestFactory())
                .build();
    }

    /** 提交/覆盖 TaskSpec(幂等 put)。 */
    public void submitSpec(TaskSpec spec) {
        try {
            rest.post().uri("/tasks").body(spec).retrieve().toBodilessEntity();
        } catch (Exception e) {
            throw new IllegalStateException("提交 TaskSpec 失败(" + spec.namespace + "/" + spec.taskId + "): "
                    + e.getMessage(), e);
        }
    }

    /** 注销 spec(框架推进 DRAINING → GONE)。 */
    public void deleteSpec(String ns, String taskId) {
        try {
            rest.delete().uri("/tasks/{ns}/{taskId}", ns, taskId).retrieve().toBodilessEntity();
        } catch (Exception e) {
            throw new IllegalStateException("注销 TaskSpec 失败(" + ns + "/" + taskId + "): " + e.getMessage(), e);
        }
    }

    /** 该 ns 全部 taskId(worker 不可达 = 空)。 */
    public List<String> listTasks(String ns) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = rest.get().uri("/tasks/{ns}", ns).retrieve().body(Map.class);
            Object o = body == null ? null : body.get("taskIds");
            if (!(o instanceof List)) return List.of();
            List<String> out = new ArrayList<>();
            for (Object id : (List<?>) o) out.add(String.valueOf(id));
            return out;
        } catch (Exception e) {
            log.warn("[worker] 列举任务失败 ns={}: {}", ns, e.getMessage());
            return List.of();
        }
    }

    /** 读 spec;worker 不可达 / 不存在 = empty。 */
    public Optional<TaskSpec> getSpec(String ns, String taskId) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = rest.get().uri("/tasks/{ns}/{taskId}/spec", ns, taskId)
                    .retrieve().body(Map.class);
            Object o = body == null ? null : body.get("spec");
            if (!(o instanceof Map)) return Optional.empty();
            return Optional.of(convert((Map<String, Object>) o, TaskSpec.class));
        } catch (Exception e) {
            log.warn("[worker] 读 spec 失败 {}/{}: {}", ns, taskId, e.getMessage());
            return Optional.empty();
        }
    }

    /** 读 status;worker 不可达 / 不存在 = empty。 */
    public Optional<TaskStatus> getStatus(String ns, String taskId) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = rest.get().uri("/tasks/{ns}/{taskId}/status", ns, taskId)
                    .retrieve().body(Map.class);
            Object o = body == null ? null : body.get("status");
            if (!(o instanceof Map)) return Optional.empty();
            return Optional.of(convert((Map<String, Object>) o, TaskStatus.class));
        } catch (Exception e) {
            log.warn("[worker] 读 status 失败 {}/{}: {}", ns, taskId, e.getMessage());
            return Optional.empty();
        }
    }

    /** 读任务事件;worker 不可达 / 不存在 = empty。 */
    public Optional<Map<String, Object>> getEvent(String ns, String taskId, String name) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = rest.get().uri("/tasks/{ns}/{taskId}/events/{name}", ns, taskId, name)
                    .retrieve().body(Map.class);
            Object o = body == null ? null : body.get("event");
            if (!(o instanceof Map)) return Optional.empty();
            return Optional.of((Map<String, Object>) o);
        } catch (Exception e) {
            log.warn("[worker] 读任务事件失败 {}/{}/{}: {}", ns, taskId, name, e.getMessage());
            return Optional.empty();
        }
    }

    /** 写任务事件(如 batch-failed 告警);幂等覆盖。 */
    public void putEvent(String ns, String taskId, String name, Map<String, Object> event) {
        try {
            rest.put().uri("/tasks/{ns}/{taskId}/events/{name}", ns, taskId, name)
                    .body(event).retrieve().toBodilessEntity();
        } catch (Exception e) {
            throw new IllegalStateException("写任务事件失败(" + ns + "/" + taskId + "/" + name + "): "
                    + e.getMessage(), e);
        }
    }

    /** 分区快照(进度页数据源);worker 不可达 = 空。 */
    public Map<Integer, ExecutorSnapshot> getSnaps(String ns, String taskId) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = rest.get().uri("/tasks/{ns}/{taskId}/snaps", ns, taskId)
                    .retrieve().body(Map.class);
            Object o = body == null ? null : body.get("snaps");
            if (!(o instanceof Map)) return Map.of();
            Map<Integer, ExecutorSnapshot> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                if (!(e.getValue() instanceof Map)) continue;
                out.put(Integer.parseInt(String.valueOf(e.getKey())),
                        convert((Map<String, Object>) e.getValue(), ExecutorSnapshot.class));
            }
            return out;
        } catch (Exception e) {
            log.warn("[worker] 读分区快照失败 {}/{}: {}", ns, taskId, e.getMessage());
            return Map.of();
        }
    }

    /** Map → 模型对象(worker 返回 JSON 经 RestClient 已是 Map,此处走 Jackson 转换)。 */
    private <T> T convert(Map<String, Object> src, Class<T> type) {
        try {
            return OM.convertValue(src, type);
        } catch (Exception e) {
            throw new IllegalStateException("响应转换失败: " + type.getSimpleName(), e);
        }
    }
}

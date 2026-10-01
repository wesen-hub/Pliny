package com.dcp.schedulerbridge;

import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.model.TaskStatus;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * dcp-worker REST 客户端(方案 §9 提交链路 / C1 ①):控制面不再直连 etcd,
 * TaskSpec 提交/注销/读状态统一走 dcp-worker。
 *
 * <pre>
 *   写: POST /tasks | DELETE /tasks/{ns}/{taskId} | PUT/DELETE /tasks/{ns}/{taskId}/events/{name}
 *       DELETE /tasks/{ns}/{taskId}/status(一次性任务终态重启)
 *   读: GET /tasks/{ns} | GET /tasks/{ns}/events/{name} | GET /tasks/{ns}/{taskId}/spec
 * </pre>
 *
 * worker 不可达时:写路径抛异常(上层回显),读路径返回空(不阻塞页面)。
 */
@Slf4j
@Component
public class WorkerClient {

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

    /** 删状态(一次性任务终态重启: 覆盖 spec 后删 status → 框架按 SUBMITTED 重新上线)。 */
    public void deleteStatus(String ns, String taskId) {
        try {
            rest.delete().uri("/tasks/{ns}/{taskId}/status", ns, taskId).retrieve().toBodilessEntity();
        } catch (Exception e) {
            throw new IllegalStateException("删任务状态失败(" + ns + "/" + taskId + "): " + e.getMessage(), e);
        }
    }

    /** 该 ns 全部 taskId(worker 不可达 = 空)。 */
    public List<String> listTasks(String ns) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = rest.get().uri("/tasks/{ns}", ns)
                    .retrieve().body(Map.class);
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

    /** 全部任务的事件(name 同名);worker 不可达 = 空。 */
    public List<Map<String, Object>> listEvents(String ns, String name) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = rest.get().uri("/tasks/{ns}/events/{name}", ns, name)
                    .retrieve().body(Map.class);
            Object o = body == null ? null : body.get("events");
            if (!(o instanceof List)) return List.of();
            List<Map<String, Object>> out = new ArrayList<>();
            for (Object item : (List<?>) o) {
                if (item instanceof Map) out.add((Map<String, Object>) item);
            }
            return out;
        } catch (Exception e) {
            log.warn("[worker] 列举任务事件失败 ns={} event={}: {}", ns, name, e.getMessage());
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

    /** 写任务事件(如 batch-failed 告警 / 扩缩容观察面);幂等覆盖。 */
    public void putEvent(String ns, String taskId, String name, Map<String, Object> event) {
        try {
            rest.put().uri("/tasks/{ns}/{taskId}/events/{name}", ns, taskId, name)
                    .body(event).retrieve().toBodilessEntity();
        } catch (Exception e) {
            throw new IllegalStateException("写任务事件失败(" + ns + "/" + taskId + "/" + name + "): "
                    + e.getMessage(), e);
        }
    }

    /** 删任务事件(扩容建议消费完成后清理)。 */
    public void deleteEvent(String ns, String taskId, String name) {
        try {
            rest.delete().uri("/tasks/{ns}/{taskId}/events/{name}", ns, taskId, name)
                    .retrieve().toBodilessEntity();
        } catch (Exception e) {
            throw new IllegalStateException("删任务事件失败(" + ns + "/" + taskId + "/" + name + "): "
                    + e.getMessage(), e);
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

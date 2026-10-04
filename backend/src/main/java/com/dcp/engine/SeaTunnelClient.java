package com.dcp.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * SeaTunnel Zeta REST API 封装（设计文档 §6.1）
 * - POST /submit-job   提交任务
 * - GET  /job-info/{id} 状态 + metrics
 * - POST /stop-job/{id} 优雅停止
 * - GET  /running-jobs  启动对账
 */
@Slf4j
@Component
public class SeaTunnelClient {

    private final String baseUrl;
    private final RestTemplate rest = new RestTemplate();
    private final ObjectMapper om = new ObjectMapper();

    public SeaTunnelClient(@Value("${dcp.seatunnel.url:http://seatunnel:8081}") String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /** 提交任务, 返回 SeaTunnel jobId（POST /submit-job?jobId=<数字> body=裸配置JSON, §6.1） */
    public String submitJob(String jobId, Map<String, Object> jobConfig) {
        return submitJob(jobId, jobConfig, false);
    }

    /**
     * 提交任务; {@code startWithSavepoint=true} 时带 {@code isStartWithSavePoint} 参数 ——
     * 同 jobId 重启时从上次 checkpoint 恢复(CDC 位点/offset 续采)。
     */
    public String submitJob(String jobId, Map<String, Object> jobConfig, boolean startWithSavepoint) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            String url = baseUrl + "/submit-job?jobId=" + jobId
                    + (startWithSavepoint ? "&isStartWithSavePoint=true" : "");
            ResponseEntity<Map> resp = rest.postForEntity(url,
                    new HttpEntity<>(toJson(jobConfig), headers), Map.class);
            if (!resp.getStatusCode().is2xxSuccessful()) {
                throw new IllegalStateException("SeaTunnel submit 失败: HTTP " + resp.getStatusCode());
            }
            Map data = resp.getBody();
            if (data != null && data.containsKey("error") && data.get("error") != null) {
                throw new IllegalStateException("SeaTunnel submit 失败: " + data.get("error"));
            }
            Object sid = data == null ? null : data.get("jobId");
            log.info("SeaTunnel 提交成功 jobId={} seatunnelJobId={}", jobId, sid);
            return String.valueOf(sid);
        } catch (Exception e) {
            throw new IllegalStateException("SeaTunnel 提交失败: " + e.getMessage(), e);
        }
    }

    /** 查询任务状态（GET /job-info/{id}, 2.3.12 REST 无 hazelcast 前缀） */
    public JobInfo jobInfo(String seatunnelJobId) {
        try {
            ResponseEntity<Map> resp = rest.getForEntity(
                    baseUrl + "/job-info/" + seatunnelJobId, Map.class);
            if (!resp.getStatusCode().is2xxSuccessful()) {
                return new JobInfo("UNKNOW", null);
            }
            Map data = resp.getBody();
            if (data == null) {
                return new JobInfo("UNKNOW", null);
            }
            return new JobInfo(str(data.get("jobStatus")), (Map<String, Object>) data.get("metrics"));
        } catch (Exception e) {
            log.warn("SeaTunnel job-info 查询异常: {}", e.getMessage());
            return new JobInfo("UNKNOW", null);
        }
    }

    public void stopJob(String seatunnelJobId) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            rest.postForEntity(baseUrl + "/stop-job",
                    new HttpEntity<>(toJson(Map.of("jobId", seatunnelJobId)), headers), Map.class);
        } catch (Exception e) {
            log.warn("SeaTunnel stop-job 异常: {}", e.getMessage());
        }
    }

    /** 运行中任务列表（对账用, GET /running-jobs） */
    @SuppressWarnings("unchecked")
    public List<String> runningJobs() {
        try {
            ResponseEntity<List> resp = rest.getForEntity(
                    baseUrl + "/running-jobs", List.class);
            List body = resp.getBody();
            if (body == null) {
                return List.of();
            }
            return body.stream()
                    .filter(j -> j instanceof Map)
                    .map(j -> str(((Map<?, ?>) j).get("jobId")))
                    .toList();
        } catch (Exception e) {
            log.warn("SeaTunnel running-jobs 查询异常: {}", e.getMessage());
            return List.of();
        }
    }

    /** 引擎整体可达性（GET /overview, 巡检用: 区分"引擎挂"与"单 job 丢失"） */
    public boolean engineReachable() {
        try {
            ResponseEntity<Map> resp = rest.getForEntity(baseUrl + "/overview", Map.class);
            return resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null;
        } catch (Exception e) {
            return false;
        }
    }

    private HttpEntity<String> jsonEntity(Object body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(toJson(body), headers);
    }

    private String toJson(Object o) {
        try {
            return om.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    @Data
    public static class JobInfo {
        private final String jobStatus;
        private final Map<String, Object> metrics;

        public boolean isFinished() {
            return "FINISHED".equalsIgnoreCase(jobStatus);
        }

        public boolean isFailed() {
            return "FAILED".equalsIgnoreCase(jobStatus) || "CANCELED".equalsIgnoreCase(jobStatus);
        }

        public boolean isRunning() {
            return "RUNNING".equalsIgnoreCase(jobStatus);
        }

        /** metrics 中取行数 */
        public long metricLong(String key) {
            if (metrics == null) {
                return 0;
            }
            Object v = metrics.get(key);
            if (v instanceof Number n) {
                return n.longValue();
            }
            if (v instanceof String s) {
                try {
                    return Long.parseLong(s);
                } catch (Exception ignored) {
                }
            }
            return 0;
        }
    }
}

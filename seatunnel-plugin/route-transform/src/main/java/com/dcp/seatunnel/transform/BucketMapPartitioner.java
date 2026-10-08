package com.dcp.seatunnel.transform;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 桶→分区自定义路由(热点桶迁移的采集侧配套, 推荐方案落层: Kafka Partitioner)。
 *
 * <p>key = {@code __bucket}(采集转换输出列, Sink {@code partition_key_fields} 指定),
 * 按 backend 元库映射返回目标分区; 实现要点:
 * <ul>
 *   <li><b>60s TTL 缓存</b>:映射权威在元库(bucket_map), 经 backend 端点
 *       {@code GET /api/sync/scale/bucket-map?topic={topic}} 拉取; TTL 窗口即迁移切换的
 *       不一致窗口(桶迁移后旧分区上的尾巴消息由消费端照常消费, seq 裁判兜底乱序);</li>
 *   <li><b>回退 Kafka 默认分区器</b>:无映射/查询失败/key 不可解析 → {@code murmur2(keyBytes) % N},
 *       与未接入本类行为完全一致(向后兼容);</li>
 *   <li><b>查询失败节流</b>:拉取失败同样记录 fetchAt(60s 内不重试), 防止 backend 不可达时每条消息都打 HTTP。</li>
 * </ul>
 *
 * <p>接线: Kafka sink producer 参数
 * {@code partitioner.class=com.dcp.seatunnel.transform.BucketMapPartitioner},
 * {@code dcp.bucket.map.baseUrl=http://backend:8081}(backend 可达地址, 引擎网络需打通)。
 */
public class BucketMapPartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(BucketMapPartitioner.class);
    private static final ObjectMapper OM = new ObjectMapper();
    private static final long TTL_MS = 60_000L;

    /** 配置键: backend baseUrl(如 http://backend:8081); 未配置则恒回退默认分区器 */
    public static final String BASE_URL_CONFIG = "dcp.bucket.map.baseUrl";

    private String baseUrl;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3)).build();
    /** topic → 桶映射(60s TTL) */
    private final Map<String, Map<Integer, Integer>> cache = new ConcurrentHashMap<>();
    /** topic → 最近拉取时间(成功/失败都记, 失败节流防风暴) */
    private final Map<String, Long> fetchedAt = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        Object v = configs.get(BASE_URL_CONFIG);
        this.baseUrl = v == null ? null : String.valueOf(v).replaceAll("/+$", "");
        log.info("BucketMapPartitioner 初始化: baseUrl={} (未配置则恒回退默认分区器)", baseUrl);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        int parts = cluster.partitionCountForTopic(topic);
        if (key == null || parts <= 1) {
            return 0;
        }
        if (baseUrl == null || baseUrl.isBlank()) {
            return defaultPartition(keyBytes, parts);
        }
        try {
            int bucket = Integer.parseInt(String.valueOf(key).trim());
            Map<Integer, Integer> map = mapOf(topic);
            Integer p = map == null ? null : map.get(bucket);
            if (p != null && p >= 0 && p < parts) {
                return p;
            }
            log.debug("桶 {} 无映射命中(新桶/映射未刷新), 回退默认分区器: {}", bucket);
        } catch (Exception ignore) {
            // key 非桶号(接入前旧消息) → 回退默认, 与未接入行为一致
        }
        return defaultPartition(keyBytes, parts);
    }

    /** Kafka 默认分区器口径: murmur2(keyBytes) % N */
    private static int defaultPartition(byte[] keyBytes, int parts) {
        int m = Utils.murmur2(keyBytes);
        return (m & 0x7fffffff) % parts;
    }

    /** 桶映射(60s TTL 缓存; 双检锁防并发击穿; 失败也节流) */
    private Map<Integer, Integer> mapOf(String topic) {
        Long last = fetchedAt.get(topic);
        if (last != null && System.currentTimeMillis() - last < TTL_MS) {
            return cache.get(topic);
        }
        synchronized (topic.intern()) {
            last = fetchedAt.get(topic);
            if (last != null && System.currentTimeMillis() - last < TTL_MS) {
                return cache.get(topic);
            }
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/api/sync/scale/bucket-map?topic=" + topic))
                        .timeout(Duration.ofSeconds(5))
                        .GET().build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    JsonNode root = OM.readTree(resp.body());
                    JsonNode mapNode = root.path("data").path("bucketMap");
                    if (mapNode.isObject()) {
                        Map<Integer, Integer> map = new ConcurrentHashMap<>();
                        Iterator<Map.Entry<String, JsonNode>> it = mapNode.fields();
                        while (it.hasNext()) {
                            Map.Entry<String, JsonNode> e = it.next();
                            try {
                                map.put(Integer.valueOf(e.getKey()), e.getValue().asInt());
                            } catch (NumberFormatException ignore) {
                            }
                        }
                        cache.put(topic, map);
                        fetchedAt.put(topic, System.currentTimeMillis());
                        return map;
                    }
                }
                log.warn("bucket-map 拉取异常 topic={} status={}", topic, resp.statusCode());
            } catch (Exception e) {
                log.warn("bucket-map 拉取失败 topic={} (60s 内不重试, 回退默认分区器): {}", topic, e.getMessage());
            }
            fetchedAt.put(topic, System.currentTimeMillis());   // 失败同样节流
            return cache.get(topic);
        }
    }

    @Override
    public void close() {
    }
}

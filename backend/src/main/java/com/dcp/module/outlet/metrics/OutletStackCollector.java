package com.dcp.module.outlet.metrics;

import com.dcp.module.outlet.OutletProperties;
import com.dcp.module.outlet.source.ChangeStreamSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 出口栈指标采集器(2026-09-14):把 {@code outlet/monitor-outlet-stack.sh} 的**口径搬进进程**,
 * 用 Spring {@code @Scheduled} 定时采集 → 注册为 Micrometer Gauge({@code chg_*} 同名、同阈值口径),
 * 于是 <b>Prometheus 只需要抓后端一个 target</b>,不必给脚本做 exporter。
 *
 * <pre>
 * chg_changefeed_up{id}                  changefeed state=normal → 1,非 normal → 0,采集失败 → -1
 * chg_changefeed_checkpoint_lag_seconds{id}  (最新 TSO - checkpoint TSO)/262144/1000
 * chg_gc_remaining_seconds               GC 安全点剩余窗口 = life_time - (now_utc - safe_point)
 * chg_kafka_isr_min{topic} / chg_kafka_replication_factor{topic} / chg_kafka_broker_up
 * outlet_consumer_offset_lag_seconds{consumer_id,stream_id}  位点滞后(追平判据)
 * </pre>
 *
 * <p>失败语义(硬约束"不许静默成功"):任一探针失败 → 对应 Gauge 登记 {@link OutletMetrics#UNAVAILABLE}(-1)
 * 并 WARN 留痕(带连续失败次数),绝不写成 0。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "dcp.outlet.monitor.enabled", havingValue = "true", matchIfMissing = true)
public class OutletStackCollector {

    public static final String M_CHANGEFEED_UP = "chg_changefeed_up";
    public static final String M_CHANGEFEED_LAG = "chg_changefeed_checkpoint_lag_seconds";
    public static final String M_GC_REMAINING = "chg_gc_remaining_seconds";
    public static final String M_ISR_MIN = "chg_kafka_isr_min";
    public static final String M_RF = "chg_kafka_replication_factor";
    public static final String M_BROKER_UP = "chg_kafka_broker_up";

    private static final DateTimeFormatter GC_SAFE_POINT_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS xx");
    private static final Pattern GC_LIFE_FMT = Pattern.compile("(?:(\\d+)h)?(?:(\\d+)m)?(?:(\\d+)s)?");

    private final OutletProperties props;
    private final OutletMetrics metrics;
    private final ObjectMapper json;
    private final ObjectProvider<JdbcTemplate> metaJdbcProvider;
    private final ObjectProvider<ChangeStreamSource> sourceProvider;

    /** 连续失败计数(探针名 → 次数),用于日志"不许静默"但又不刷屏 */
    private final Map<String, Integer> failStreak = new ConcurrentHashMap<>();
    /** 最近一次采集快照(给详情页接口读"当前值",不依赖 Prometheus) */
    private volatile Map<String, Object> lastSnapshot = Map.of();

    /** Kafka 引导地址兜底(= spring.kafka.bootstrap-servers;compose 已指向 3 broker 新集群) */
    @Value("${spring.kafka.bootstrap-servers:}")
    private String kafkaBootstrapFallback = "";

    public OutletStackCollector(OutletProperties props, OutletMetrics metrics,
                                ObjectProvider<ObjectMapper> jsonProvider,
                                ObjectProvider<JdbcTemplate> metaJdbcProvider,
                                ObjectProvider<ChangeStreamSource> sourceProvider) {
        this.props = props;
        this.metrics = metrics;
        this.json = jsonProvider.getIfAvailable(ObjectMapper::new);
        this.metaJdbcProvider = metaJdbcProvider;
        this.sourceProvider = sourceProvider;
    }

    /* ------------------------------------------------------------------ 定时采集 */

    @Scheduled(fixedDelayString = "${dcp.outlet.monitor.interval-ms:20000}",
            initialDelayString = "${dcp.outlet.monitor.initial-delay-ms:20000}")
    public void collect() {
        OutletProperties.Monitor m = props.getMonitor();
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("collectedAtMillis", System.currentTimeMillis());

        /* ① TiCDC changefeed:state + checkpoint 滞后 */
        Changefeed cf = probeChangefeed(m);
        Map<String, String> cfTags = Map.of("id", m.getChangefeedId());
        metrics.setStackGauge(M_CHANGEFEED_UP, cfTags, cf.probeOk ? ("normal".equals(cf.state) ? 1d : 0d) : OutletMetrics.UNAVAILABLE);
        metrics.setStackGauge(M_CHANGEFEED_LAG, cfTags, cf.lagSeconds);
        snap.put("changefeed", Map.of(
                "id", m.getChangefeedId(),
                "state", cf.state,
                "probeOk", cf.probeOk,
                "checkpointTso", cf.checkpointTso,
                "lagSeconds", cf.lagSeconds));

        /* ② TiDB GC 剩余窗口 */
        Gc gc = probeGc(m);
        metrics.setStackGauge(M_GC_REMAINING, Map.of(), gc.remainingSeconds);
        snap.put("gc", Map.of("probeOk", gc.probeOk, "remainingSeconds", gc.remainingSeconds,
                "lifeTime", gc.lifeTime, "safePoint", gc.safePoint));

        /* ③ Kafka topic RF / 最小 ISR / 存活 broker 数 */
        Kafka kafka = probeKafka(m);
        Map<String, String> kafkaTags = Map.of("topic", m.getTopic());
        metrics.setStackGauge(M_ISR_MIN, kafkaTags, kafka.isrMin);
        metrics.setStackGauge(M_RF, kafkaTags, kafka.rf);
        metrics.setStackGauge(M_BROKER_UP, Map.of(), kafka.brokersUp);
        snap.put("kafka", Map.of("probeOk", kafka.probeOk, "topic", m.getTopic(),
                "isrMin", kafka.isrMin, "rf", kafka.rf, "brokersUp", kafka.brokersUp));

        /* ④ 消费方位点滞后(逐消费方/流;追平判据) */
        List<Map<String, Object>> lags = collectConsumerLags();
        snap.put("consumers", lags);

        snap.put("thresholds", Map.of(
                "lagWarnSeconds", m.getLagWarnSeconds(),
                "lagCritSeconds", m.getLagCritSeconds(),
                "gcWarnSeconds", m.getGcWarnSeconds()));
        lastSnapshot = snap;

        log.info("[outlet-metrics] 采集完成 changefeed={}(probe={}) lag={}s | gcRemaining={}s | isrMin={} rf={} brokers={} | 消费方滞后={}",
                cf.state, cf.probeOk, fmt(cf.lagSeconds), fmt(gc.remainingSeconds),
                fmt(kafka.isrMin), fmt(kafka.rf), fmt(kafka.brokersUp), lags);
    }

    /** 最近一次采集快照(供详情页接口;无 Prometheus 也能读当前值) */
    public Map<String, Object> lastSnapshot() {
        return lastSnapshot;
    }

    /* ------------------------------------------------------------------ 探针实现 */

    private record Changefeed(boolean probeOk, String state, long checkpointTso, double lagSeconds) {}

    private Changefeed probeChangefeed(OutletProperties.Monitor m) {
        String url = m.getTicdcUrl() + "/api/v1/changefeeds/" + m.getChangefeedId();
        try {
            JsonNode n = json.readTree(httpGet(url));
            String state = n.path("state").asText("unknown");
            long checkpoint = n.path("checkpoint_tso").asLong(0);
            long latest = tidbTso();
            double lag = latest > 0 && checkpoint > 0
                    ? Math.round((latest - checkpoint) / OutletMetrics.TSO_TICKS_PER_SECOND * 10d) / 10d
                    : OutletMetrics.UNAVAILABLE;
            ok("changefeed");
            return new Changefeed(true, state, checkpoint, lag);
        } catch (Exception e) {
            fail("changefeed", e);
            return new Changefeed(false, "unavailable", 0, OutletMetrics.UNAVAILABLE);
        }
    }

    private record Gc(boolean probeOk, double remainingSeconds, String lifeTime, String safePoint) {}

    private Gc probeGc(OutletProperties.Monitor m) {
        String lifeTime = null;
        String safePoint = null;
        try {
            try (Connection c = tidbConnection();
                 Statement st = c.createStatement()) {
                try (ResultSet rs = st.executeQuery(
                        "SELECT variable_value FROM mysql.tidb WHERE variable_name='tikv_gc_safe_point'")) {
                    if (rs.next()) {
                        safePoint = rs.getString(1);
                    }
                }
                try (ResultSet rs = st.executeQuery("SELECT @@tidb_gc_life_time")) {
                    if (rs.next()) {
                        lifeTime = rs.getString(1);
                    }
                }
            }
            if (safePoint == null || lifeTime == null) {
                throw new IllegalStateException("GC 变量缺失 safePoint=" + safePoint + " lifeTime=" + lifeTime);
            }
            long lifeSec = parseLifeTime(lifeTime);
            OffsetDateTime sp = OffsetDateTime.parse(safePoint.trim(), GC_SAFE_POINT_FMT);
            double remaining = lifeSec - (OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond() - sp.toEpochSecond());
            ok("gc");
            return new Gc(true, remaining, lifeTime, safePoint);
        } catch (Exception e) {
            fail("gc", e);
            return new Gc(false, OutletMetrics.UNAVAILABLE, lifeTime, safePoint);
        }
    }

    private record Kafka(boolean probeOk, int isrMin, int rf, int brokersUp) {}

    private Kafka probeKafka(OutletProperties.Monitor m) {
        String bootstrap = kafkaBootstrap(m.getKafkaBootstrap());
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "dcp-outlet-metrics");
        try (AdminClient admin = AdminClient.create(p)) {
            DescribeTopicsResult dtr = admin.describeTopics(List.of(m.getTopic()));
            TopicDescription td = dtr.allTopicNames().get(8, TimeUnit.SECONDS).get(m.getTopic());
            int rf = 0;
            int isrMin = Integer.MAX_VALUE;
            for (TopicPartitionInfo part : td.partitions()) {
                rf = Math.max(rf, part.replicas().size());
                isrMin = Math.min(isrMin, part.isr().size());
            }
            int brokers = admin.describeCluster().nodes().get(8, TimeUnit.SECONDS).size();
            ok("kafka");
            return new Kafka(true, isrMin == Integer.MAX_VALUE ? 0 : isrMin, rf, brokers);
        } catch (Exception e) {
            fail("kafka", e);
            return new Kafka(false, -1, -1, -1);
        }
    }

    /**
     * 逐 (consumer_id, stream_id) 计算位点滞后 = watermark − 已处理位点(秒)。
     * 位点读元库 {@code consumer_offset}(演示/内部消费方;生产位点在消费方自己的库)。
     */
    private List<Map<String, Object>> collectConsumerLags() {
        List<Map<String, Object>> out = new java.util.ArrayList<>();
        JdbcTemplate jdbc = metaJdbcProvider.getIfAvailable();
        ChangeStreamSource source = sourceProvider.getIfAvailable();
        if (jdbc == null) {
            fail("consumer-offset", new IllegalStateException("无 JdbcTemplate(元库)"));
            return out;
        }
        List<Map<String, Object>> rows;
        try {
            rows = jdbc.queryForList("SELECT consumer_id, stream_id, epoch, position_tso FROM consumer_offset");
            ok("consumer-offset");
        } catch (Exception e) {
            fail("consumer-offset", e);
            return out;
        }
        for (Map<String, Object> r : rows) {
            String consumerId = String.valueOf(r.get("consumer_id"));
            String streamId = String.valueOf(r.get("stream_id"));
            long position = toLong(r.get("position_tso"));
            double lag = OutletMetrics.UNAVAILABLE;
            Long watermark = null;
            if (source != null) {
                try {
                    watermark = source.watermark(streamId);
                } catch (RuntimeException e) {
                    fail("watermark:" + streamId, e);
                }
            }
            if (watermark != null && watermark > 0) {
                lag = Math.max(0d, Math.round((watermark - position) / OutletMetrics.TSO_TICKS_PER_SECOND * 10d) / 10d);
            } else {
                fail("watermark:" + streamId, new IllegalStateException("无水位线(watermark=null),滞后登记为 -1"));
            }
            metrics.setConsumerLag(consumerId, streamId, lag);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("consumerId", consumerId);
            item.put("streamId", streamId);
            item.put("epoch", toLong(r.get("epoch")));
            item.put("positionTso", position);
            item.put("watermarkTso", watermark);
            item.put("lagSeconds", lag);
            out.add(item);
        }
        return out;
    }

    /* ------------------------------------------------------------------ 基础设施小事 */

    /** 显式配置 → spring.kafka.bootstrap-servers → 新集群默认值 三级兜底 */
    private String kafkaBootstrap(String configured) {
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        if (kafkaBootstrapFallback != null && !kafkaBootstrapFallback.isBlank()) {
            return kafkaBootstrapFallback;
        }
        return "dcp-outlet-kafka1:9092,dcp-outlet-kafka2:9092,dcp-outlet-kafka3:9092";
    }

    private String httpGet(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        conn.setRequestMethod("GET");
        try {
            int code = conn.getResponseCode();
            InputStream in = code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream();
            String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
            if (code < 200 || code >= 300) {
                throw new IllegalStateException("HTTP " + code + " " + url + " body=" + trim(body));
            }
            return body;
        } finally {
            conn.disconnect();
        }
    }

    private long tidbTso() {
        try (Connection c = tidbConnection(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SHOW MASTER STATUS")) {
                if (rs.next()) {
                    // TiDB: File=tidb-binlog, Position=当前 TSO
                    return toLong(rs.getObject("Position"));
                }
            }
            return -1L;
        } catch (Exception e) {
            fail("tidb-tso", e);
            return -1L;
        }
    }

    private Connection tidbConnection() throws Exception {
        OutletProperties.Tidb t = props.getTidb();
        DriverManager.setLoginTimeout(5);
        return DriverManager.getConnection(t.getUrl(), t.getUsername(), t.getPassword());
    }

    private static long parseLifeTime(String lifeTime) {
        Matcher mt = GC_LIFE_FMT.matcher(lifeTime.trim());
        if (!mt.find()) {
            return 0L;
        }
        long h = mt.group(1) == null ? 0 : Long.parseLong(mt.group(1));
        long mi = mt.group(2) == null ? 0 : Long.parseLong(mt.group(2));
        long s = mt.group(3) == null ? 0 : Long.parseLong(mt.group(3));
        return h * 3600 + mi * 60 + s;
    }

    /** 探针成功:清零连续失败计数 */
    private void ok(String probe) {
        Integer prev = failStreak.remove(probe);
        if (prev != null) {
            log.info("[outlet-metrics] 探针恢复 probe={} 此前连续失败 {} 次", probe, prev);
        }
    }

    /** 探针失败:登记 -1 口径 + WARN 留痕(带连续失败次数,便于判"持续故障"而不是偶发) */
    private void fail(String probe, Exception e) {
        int n = failStreak.merge(probe, 1, Integer::sum);
        log.warn("[outlet-metrics] 探针失败 probe={} 连续失败={} → 指标登记 -1 err={}", probe, n, e.toString());
    }

    private static long toLong(Object o) {
        if (o == null) {
            return 0L;
        }
        if (o instanceof Number num) {
            return num.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(o));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static String fmt(double v) {
        return v == Math.rint(v) ? String.valueOf((long) v) : String.valueOf(v);
    }

    private static String trim(String s) {
        return s == null ? "" : (s.length() > 200 ? s.substring(0, 200) + "…" : s);
    }
}

package com.dcp.sync.ingest;

import com.dcp.sync.CdcRecord;
import com.dcp.sync.LogicalTableConfigService;
import com.dcp.sync.RouteUtil;
import com.dcp.sync.BucketMapService;
import com.dcp.sync.SeqAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Kafka 接入引擎(2026-09-06 重新设计 — 应用/凭证模型):
 *
 * <pre>
 *   页面: 新建「接入应用」→ 平台颁发 appKey(创建即启用, 白名单绑定逻辑表)
 *   对方: SDK/HTTP 携带 appKey 提交消息(op 在数据包内指定)
 *   HTTP: 401 key 无效/停用 → 403 未绑定该表 → 400 预检(op/字段/类型/业务键) → 200 入队
 *   dcp.in.{逻辑表}(削峰) → 消费: 验证 → DLT(source_table=appKey) / 通过: 领 seq + 信封 → dcp.{逻辑表}
 *   监控: 按应用聚合 ok/dlt(分表)、表级 lag; 应用停用=禁提交, 无启用应用的表自动停消费者
 * </pre>
 */
@Slf4j
@Component
public class KafkaIngestEngine {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final SecureRandom RAND = new SecureRandom();
    private static final DateTimeFormatter HM = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static final String IN_TOPIC_PREFIX = "dcp.in.";

    private final JdbcTemplate metaJdbc;
    private final KafkaProperties kafkaProperties;
    private final KafkaTemplate<String, String> kafka;
    private final SeqAllocator seqAllocator;
    private final LogicalTableConfigService tableConfigService;
    private final BucketMapService bucketMapService;

    /** 逻辑表 → 运行容器(共享: 每逻辑表一个入站消费者) */
    private final Map<String, Runtime> runtimes = new ConcurrentHashMap<>();
    /** 指标: 应用 → 累计 ok/dlt + 每秒采样环; 细表: appKey|lt → {ok,dlt} */
    private final Map<String, Metrics> metrics = new ConcurrentHashMap<>();
    private final Map<String, long[]> tableOk = new ConcurrentHashMap<>();

    @Autowired
    public KafkaIngestEngine(JdbcTemplate metaJdbc, KafkaProperties kafkaProperties,
                             KafkaTemplate<String, String> kafka, SeqAllocator seqAllocator,
                             LogicalTableConfigService tableConfigService,
                             BucketMapService bucketMapService) {
        this.metaJdbc = metaJdbc;
        this.kafkaProperties = kafkaProperties;
        this.kafka = kafka;
        this.seqAllocator = seqAllocator;
        this.tableConfigService = tableConfigService;
        this.bucketMapService = bucketMapService;
    }

    // ────────────────────────── 表结构与启动 ──────────────────────────

    @PostConstruct
    public void ensureTable() {
        // 重新设计换代: 旧"每逻辑表任务+默认op"模型废弃
        metaJdbc.execute("DROP TABLE IF EXISTS kafka_ingest_task");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS kafka_app ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "app_name VARCHAR(128) NOT NULL,"
                + "app_key VARCHAR(64) NOT NULL UNIQUE,"
                + "status VARCHAR(16) DEFAULT 'ENABLED',"
                + "description VARCHAR(255),"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP)");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS kafka_app_logical ("
                + "app_id BIGINT NOT NULL,"
                + "logical_table VARCHAR(128) NOT NULL,"
                + "UNIQUE KEY uk_app_lt (app_id, logical_table))");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        for (String lt : activeTables()) {
            try {
                start(lt);
            } catch (Exception e) {
                log.error("Kafka 接入入站消费启动失败: {} - {}", lt, e.getMessage());
            }
        }
        log.info("Kafka 接入引擎就绪(应用/凭证模型)");
    }

    @PreDestroy
    public void stopAll() {
        for (Runtime rt : new ArrayList<>(runtimes.values())) {
            try {
                if (rt.container != null) {
                    rt.container.stop(true);
                }
            } catch (Exception ignored) {
            }
        }
        runtimes.clear();
    }

    /** 有 ≥1 个启用应用绑定的逻辑表 */
    private List<String> activeTables() {
        return metaJdbc.queryForList(
                "SELECT DISTINCT l.logical_table FROM kafka_app_logical l "
                        + "JOIN kafka_app a ON a.id = l.app_id WHERE a.status = 'ENABLED'",
                String.class);
    }

    // ────────────────────────── 应用(任务)管理 ──────────────────────────

    /** 新建接入应用: 颁发 appKey(只显示一次), 绑定逻辑表白名单, 创建即启用 */
    public Map<String, Object> createApp(String appName, String description, List<String> logicalTables) {
        if (appName == null || appName.isBlank()) {
            throw new IllegalArgumentException("应用名称(业务系统名)不能为空");
        }
        String appKey = "dcp-" + Long.toHexString(System.currentTimeMillis())
                + Integer.toHexString(RAND.nextInt(0xFFFFFF));
        if (logicalTables == null || logicalTables.isEmpty()) {
            throw new IllegalArgumentException("至少绑定一个逻辑表(白名单)");
        }
        for (String lt : logicalTables) {
            Long n = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM logical_table WHERE table_name = ?", Long.class, lt);
            if (n == null || n == 0) {
                throw new IllegalArgumentException("逻辑表不存在(请先在模型管理发布): " + lt);
            }
        }
        metaJdbc.update("INSERT INTO kafka_app (app_name, app_key, status, description) VALUES (?, ?, 'ENABLED', ?)",
                appName, appKey, description);
        Long appId = metaJdbc.queryForObject("SELECT id FROM kafka_app WHERE app_key = ?", Long.class, appKey);
        for (String lt : logicalTables) {
            metaJdbc.update("INSERT IGNORE INTO kafka_app_logical (app_id, logical_table) VALUES (?, ?)", appId, lt);
        }
        for (String lt : logicalTables) {
            start(lt);
        }
        log.info("Kafka 接入应用创建: {} key={} tables={}", appName, appKey, logicalTables);
        return Map.of("appKey", appKey, "appName", appName, "logicalTables", logicalTables);
    }

    public List<Map<String, Object>> listApps() {
        List<Map<String, Object>> apps = metaJdbc.queryForList(
                "SELECT id, app_name, app_key, status, description, create_time FROM kafka_app ORDER BY id DESC");
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> a : apps) {
            String key = String.valueOf(a.get("app_key"));
            List<String> tables = tablesOfApp(((Number) a.get("id")).longValue());
            Map<String, Object> row = new LinkedHashMap<>(a);
            row.put("logicalTables", tables);
            row.put("ok", appOk(key));
            row.put("dlt", dltCount(key, null));
            row.put("running", tables.stream().anyMatch(runtimes::containsKey));
            out.add(row);
        }
        return out;
    }

    public Map<String, Object> appStatus(String appKey) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id, app_name, app_key, status, description FROM kafka_app WHERE app_key = ?", appKey);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("接入应用不存在: " + appKey);
        }
        Map<String, Object> app = rows.get(0);
        long appId = ((Number) app.get("id")).longValue();
        Metrics m = metrics.computeIfAbsent(appKey, k -> new Metrics());
        List<Map<String, Object>> tables = new ArrayList<>();
        for (String lt : tablesOfApp(appId)) {
            long[] oklt = tableOk.getOrDefault(appKey + "|" + lt, new long[2]);
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("logicalTable", lt);
            t.put("ok", oklt[0]);
            t.put("dlt", dltCount(appKey, lt));
            t.put("lag", lagOf("dcp-ingest-" + lt, IN_TOPIC_PREFIX + lt));
            t.put("running", runtimes.containsKey(lt));
            tables.add(t);
        }
        Map<String, Object> out = new LinkedHashMap<>(app);
        out.put("tables", tables);
        out.put("ok", appOk(appKey));
        out.put("dlt", dltCount(appKey, null));
        out.put("samples", m.samples());
        return out;
    }

    public void enableApp(String appKey, boolean enable) {
        int n = metaJdbc.update("UPDATE kafka_app SET status = ? WHERE app_key = ?",
                enable ? "ENABLED" : "DISABLED", appKey);
        if (n == 0) {
            throw new IllegalArgumentException("接入应用不存在: " + appKey);
        }
        reconcile(metaJdbc.queryForList(
                "SELECT logical_table FROM kafka_app_logical WHERE app_id = "
                        + "(SELECT id FROM kafka_app WHERE app_key = ?)", String.class, appKey));
        log.info("Kafka 接入应用 {}: {}", appKey, enable ? "启用" : "停用");
    }

    public void deleteApp(String appKey) {
        List<String> tables = metaJdbc.queryForList(
                "SELECT logical_table FROM kafka_app_logical WHERE app_id = (SELECT id FROM kafka_app WHERE app_key = ?)",
                String.class, appKey);
        metaJdbc.update("DELETE FROM kafka_app_logical WHERE app_id = (SELECT id FROM kafka_app WHERE app_key = ?)", appKey);
        metaJdbc.update("DELETE FROM kafka_app WHERE app_key = ?", appKey);
        metrics.remove(appKey);
        reconcile(tables);
    }

    /** 重绑白名单(整体替换) */
    public void bindTables(String appKey, List<String> logicalTables) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id FROM kafka_app WHERE app_key = ?", appKey);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("接入应用不存在: " + appKey);
        }
        long appId = ((Number) rows.get(0).get("id")).longValue();
        List<String> old = tablesOfApp(appId);
        metaJdbc.update("DELETE FROM kafka_app_logical WHERE app_id = ?", appId);
        for (String lt : logicalTables) {
            metaJdbc.update("INSERT IGNORE INTO kafka_app_logical (app_id, logical_table) VALUES (?, ?)", appId, lt);
        }
        List<String> union = new ArrayList<>(old);
        for (String lt : logicalTables) {
            if (!union.contains(lt)) {
                union.add(lt);
            }
        }
        reconcile(union);
    }

    private List<String> tablesOfApp(long appId) {
        return metaJdbc.queryForList(
                "SELECT logical_table FROM kafka_app_logical WHERE app_id = ? ORDER BY logical_table", String.class, appId);
    }

    /** 停用/删除/重绑后: 无启用应用的表停消费者, 有则确保在跑 */
    private void reconcile(List<String> tables) {
        for (String lt : new HashSet<>(tables)) {
            Long enabled = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM kafka_app_logical l JOIN kafka_app a ON a.id = l.app_id "
                            + "WHERE l.logical_table = ? AND a.status = 'ENABLED'", Long.class, lt);
            if (enabled == null || enabled == 0) {
                stop(lt);
            } else {
                start(lt);
            }
        }
    }

    // ────────────────────────── 提交(鉴权 + 预检) ──────────────────────────

    /**
     * HTTP/SDK 提交: 401 key 无效/停用 → 403 未绑定 → 400 预检 → 200 入 dcp.in.{逻辑表}。
     * op 由消息包内指定(缺省降级 INSERT, 页面零配置)。
     */
    public void submit(String logicalTable, String appKey, String op, String ts,
                       Map<String, Object> data) {
        String status = queryStatus(appKey);
        if (status == null) {
            throw new AuthException("appKey 无效: " + appKey);
        }
        if (!"ENABLED".equals(status)) {
            throw new AuthException("接入应用已停用: " + appKey);
        }
        Long bound = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM kafka_app_logical l JOIN kafka_app a ON a.id = l.app_id "
                        + "WHERE a.app_key = ? AND l.logical_table = ?", Long.class, appKey, logicalTable);
        if (bound == null || bound == 0) {
            throw new ForbiddenException("appKey 未绑定该逻辑表(白名单): " + logicalTable);
        }
        Analysis a = analyze(logicalTable, op == null ? "INSERT" : op, data);
        if (a.error != null) {
            throw new IllegalArgumentException("预检未通过: " + a.error);
        }
        String value = buildInbound(a.op, ts, appKey, data);
        kafka.send(IN_TOPIC_PREFIX + logicalTable, a.businessKey, value);
        log.info("Kafka 接入提交: {}({}) appKey={} key={}", logicalTable, a.op, appKey, a.businessKey);
    }

    /** SDK bootstrap: 业务键字段 + 入站 topic(需鉴权) */
    public Map<String, Object> meta(String logicalTable, String appKey) {
        if (queryStatus(appKey) == null) {
            throw new AuthException("appKey 无效: " + appKey);
        }
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("logicalTable", logicalTable);
        out.put("keyColumns", cfg.getKeyColumns() == null ? List.of() : cfg.getKeyColumns());
        out.put("inTopic", IN_TOPIC_PREFIX + logicalTable);
        return out;
    }

    private String queryStatus(String appKey) {
        try {
            return metaJdbc.queryForObject(
                    "SELECT status FROM kafka_app WHERE app_key = ?", String.class, appKey);
        } catch (Exception e) {
            return null;
        }
    }

    // ────────────────────────── 入站消费(每逻辑表共享) ──────────────────────────

    public synchronized void start(String logicalTable) {
        if (runtimes.containsKey(logicalTable)) {
            return;
        }
        ensureInTopic(logicalTable);
        String inTopic = IN_TOPIC_PREFIX + logicalTable;
        DefaultKafkaConsumerFactory<String, String> factory =
                new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(null));
        ContainerProperties props = new ContainerProperties(inTopic);
        props.setGroupId("dcp-ingest-" + logicalTable);
        props.setAckMode(ContainerProperties.AckMode.MANUAL);
        props.setMessageListener((BatchConsumerAwareMessageListener<String, String>) (records, consumer) -> {
            handleBatch(logicalTable, records, consumer);
        });
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(factory, props);
        container.setConcurrency(1);
        container.start();
        runtimes.put(logicalTable, new Runtime(container));
        log.info("Kafka 接入入站消费已启动: {} (group=dcp-ingest-{})", inTopic, logicalTable);
    }

    public synchronized void stop(String logicalTable) {
        Runtime rt = runtimes.remove(logicalTable);
        if (rt != null) {
            try {
                rt.container.stop(true);
            } catch (Exception e) {
                log.warn("停止入站消费异常: {} - {}", logicalTable, e.getMessage());
            }
        }
    }

    private void handleBatch(String logicalTable, List<ConsumerRecord<String, String>> records,
                             Consumer<?, ?> consumer) {
        if (records.isEmpty()) {
            return;
        }
        // 按提交方(appKey)分组: 各自领号(写者 seq 列独立) + 各自计数
        Map<String, List<Object[]>> byApp = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            String appKey;
            String opDefault;
            Map<String, Object> data;
            try {
                JsonNode node = OM.readTree(record.value());
                if (node.has("submitter")) {
                    appKey = node.get("submitter").asText("unknown");
                } else {
                    appKey = "unknown";
                }
                opDefault = "INSERT";
                if (node.has("op")) {
                    opDefault = node.get("op").asText();
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> dataMap = node.has("data") && node.get("data").isObject()
                        ? OM.convertValue(node.get("data"), Map.class)
                        : OM.convertValue(node, Map.class);
                data = new LinkedHashMap<>(dataMap);
                data.remove("submitter");
                data.remove("op");
                data.remove("ts");
            } catch (Exception e) {
                appKey = "unknown";
                opDefault = "INSERT";
                data = new LinkedHashMap<>();
                dlt(logicalTable, appKey, record, "入站消息解析失败: " + e.getMessage());
                mark(appKey, logicalTable, false);
                continue;
            }
            Analysis a = analyze(logicalTable, opDefault, data);
            if (a.error != null) {
                dlt(logicalTable, appKey, record, a.error);
                mark(appKey, logicalTable, false);
                continue;
            }
            byApp.computeIfAbsent(appKey, k -> new ArrayList<>())
                    .add(new Object[]{a.businessKey, a.op, a.fields});
        }
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        String topic = cfg.getTopic();
        int bucketCount = cfg.getBucketCount() > 0 ? cfg.getBucketCount() : 4096;
        List<String> keyColumns = cfg.getKeyColumns() == null ? List.of() : cfg.getKeyColumns();
        for (Map.Entry<String, List<Object[]>> e : byApp.entrySet()) {
            String appKey = e.getKey();
            List<Object[]> recs = e.getValue();
            if (recs.isEmpty()) {
                continue;
            }
            // 每 (应用×逻辑表) 独立 seq 计数行 → 写者列独立, 各比各的
            long[] range = seqAllocator.allocate("kafka|" + logicalTable + "|" + appKey, recs.size());
            long seq = range[0];
            String taskId = "k" + appKey.replaceAll("[^A-Za-z0-9_]", "_");
            for (Object[] rec : recs) {
                String businessKey = (String) rec[0];
                String rowKind = (String) rec[1];
                @SuppressWarnings("unchecked")
                Map<String, Object> fields = (Map<String, Object>) rec[2];
                int bucket = Math.floorMod(RouteUtil.hash(businessKey), bucketCount);
                int partition = bucketMapService.partitionOf(logicalTable, bucket);
                CdcRecord cdc = CdcRecord.of(logicalTable, businessKey, keyColumns, rowKind, seq, fields);
                cdc.setTaskId(taskId);
                cdc.setSourceDb("kafka");
                cdc.setSourceTable("ingest");
                cdc.setDatasourceId("k");
                try {
                    kafka.send(topic, partition, businessKey, com.dcp.sync.SyncProducer.Json.toJson(cdc)).get();
                } catch (Exception ex) {
                    // 投递失败: 不提交 offset → 整批重投(at-least-once, 目标幂等); seq 重领新号段
                    throw new IllegalStateException("Kafka 接入写 dcp 失败 topic=" + topic
                            + " cause=" + ex.getMessage(), ex);
                }
                seq++;
            }
            mark(appKey, logicalTable, true, recs.size());
        }
        tick(logicalTable, records);
        commit(consumer);
    }

    /** 指标: 应用级 ok/dlt + 分表 ok + 采样 */
    private void mark(String appKey, String logicalTable, boolean ok, int... n) {
        Metrics m = metrics.computeIfAbsent(appKey, k -> new Metrics());
        if (ok) {
            int cnt = n.length > 0 ? n[0] : 1;
            m.ok += cnt;
            long[] t = tableOk.computeIfAbsent(appKey + "|" + logicalTable, k -> new long[2]);
            t[0] += cnt;
        } else {
            m.dlt++;
            long[] t = tableOk.computeIfAbsent(appKey + "|" + logicalTable, k -> new long[2]);
            t[1]++;
        }
    }

    private void tick(String logicalTable, List<ConsumerRecord<String, String>> records) {
        Set<String> apps = new HashSet<>();
        for (ConsumerRecord<String, String> r : records) {
            try {
                apps.add(OM.readTree(r.value()).path("submitter").asText("unknown"));
            } catch (Exception ignored) {
                apps.add("unknown");
            }
        }
        for (String appKey : apps) {
            Metrics m = metrics.computeIfAbsent(appKey, k -> new Metrics());
            m.tick();
        }
        // 未识别 submitter 的单独环(理论上不可达; 兜底)
        if (apps.isEmpty()) {
            metrics.computeIfAbsent("unknown", k -> new Metrics()).tick();
        }
    }

    private void commit(Consumer<?, ?> consumer) {
        try {
            consumer.commitSync(java.time.Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("Kafka 接入 offset 提交失败: {}", e.getMessage());
        }
    }

    // ────────────────────────── 验证器(HTTP 预检与消费端同源) ──────────────────────────

    public Analysis analyze(String logicalTable, String op, Map<String, Object> data) {
        String normOp = normalizeOp(op);
        if (normOp == null) {
            return new Analysis("op 非法(仅 I/U/D 或 INSERT/UPDATE/DELETE): " + op);
        }
        if (data == null) {
            return new Analysis("data 不能为空(JSON 对象)");
        }
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        List<String> keyCols = cfg.getKeyColumns() == null ? List.of() : cfg.getKeyColumns();
        Map<String, String> columns = loadColumns(logicalTable);
        StringBuilder key = new StringBuilder();
        for (String k : keyCols) {
            Object v = data.get(k);
            if (v == null || String.valueOf(v).isBlank()) {
                return new Analysis("业务键字段缺失或为空: " + k);
            }
            if (key.length() > 0) {
                key.append('|');
            }
            key.append(v);
        }
        String businessKey = key.length() == 0 ? null : key.toString();
        if (businessKey == null) {
            return new Analysis("逻辑表未配置业务键(key_columns 为空)");
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : data.entrySet()) {
            String name = e.getKey();
            String colType = columns.get(name);
            if (columns.isEmpty() || colType != null) {
                if (colType != null && isNumericType(colType) && e.getValue() != null
                        && !String.valueOf(e.getValue()).isBlank()) {
                    try {
                        new BigDecimal(String.valueOf(e.getValue()));
                    } catch (NumberFormatException ex) {
                        return new Analysis("字段类型不可转换: " + name + "=" + e.getValue() + " (需要 " + colType + ")");
                    }
                }
                fields.put(name, e.getValue() == null ? null : String.valueOf(e.getValue()));
            }
        }
        return new Analysis(normOp, businessKey, fields, null);
    }

    private Map<String, String> loadColumns(String logicalTable) {
        Map<String, String> cols = new LinkedHashMap<>();
        List<String> fieldsJson = metaJdbc.queryForList(
                "SELECT fields FROM model_definition WHERE table_name = ? ORDER BY id DESC LIMIT 1",
                String.class, logicalTable);
        if (fieldsJson.isEmpty()) {
            return cols;
        }
        try {
            JsonNode arr = OM.readTree(fieldsJson.get(0));
            if (arr.isArray()) {
                for (JsonNode n : arr) {
                    cols.put(n.path("name").asText(), n.path("type").asText());
                }
            }
        } catch (Exception e) {
            log.warn("模型字段解析失败 {}: {}", logicalTable, e.getMessage());
        }
        return cols;
    }

    private static boolean isNumericType(String type) {
        String t = type == null ? "" : type.toLowerCase(Locale.ROOT);
        return t.contains("int") || t.contains("decimal") || t.contains("double")
                || t.contains("float") || t.contains("numeric");
    }

    private static String normalizeOp(String op) {
        if (op == null) {
            return null;
        }
        String u = op.trim().toUpperCase(Locale.ROOT);
        switch (u) {
            case "I":
            case "INSERT":
                return "INSERT";
            case "U":
            case "UPDATE":
                return "UPDATE";
            case "D":
            case "DELETE":
                return "DELETE";
            default:
                return null;
        }
    }

    // ────────────────────────── 入站消息 / DLT / topic / lag / 计数 ──────────────────────────

    private String buildInbound(String op, String ts, String appKey, Map<String, Object> data) {
        ObjectNode node = OM.createObjectNode();
        node.put("op", op);
        node.put("ts", ts == null || ts.isBlank()
                ? LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : ts);
        node.put("submitter", appKey);
        node.set("data", OM.valueToTree(data));
        return node.toString();
    }

    private void dlt(String logicalTable, String appKey, ConsumerRecord<String, String> record, String error) {
        try {
            metaJdbc.update(
                    "INSERT INTO dlt_message (logical_table, source_table, topic, partition_id, offset_id, "
                            + "business_key, payload, error_msg, fail_count, status, first_fail_time, last_fail_time) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'PENDING', NOW(), NOW())",
                    logicalTable, appKey, record.topic(), record.partition(), record.offset(),
                    record.key(), record.value(), error.length() > 2000 ? error.substring(0, 2000) : error);
        } catch (Exception e) {
            log.error("Kafka 接入 DLT 落库失败: {}", e.getMessage());
        }
    }

    private void ensureInTopic(String logicalTable) {
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        String topic = IN_TOPIC_PREFIX + logicalTable;
        int partitions = cfg.getPartitionCount() > 0 ? cfg.getPartitionCount() : 8;
        try (AdminClient admin = AdminClient.create(kafkaProperties.buildAdminProperties(null))) {
            var existing = admin.describeTopics(List.of(topic)).allTopicNames()
                    .get(5, TimeUnit.SECONDS).get(topic);
            if (existing == null) {
                try {
                    admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                            .all().get(30, TimeUnit.SECONDS);
                    log.info("Kafka 接入 topic 已创建: {} partitions={}", topic, partitions);
                } catch (Exception e) {
                    log.warn("Kafka 接入 topic 创建竞态(待校验): {} - {}", topic, e.getMessage());
                }
            }
            for (int i = 0; i < 5 && partitions > 1; i++) {
                int cur = admin.describeTopics(List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS)
                        .get(topic).partitions().size();
                if (cur >= partitions) {
                    break;
                }
                try {
                    admin.createPartitions(Map.of(topic,
                                    org.apache.kafka.clients.admin.NewPartitions.increaseTo(partitions)))
                            .all().get(15, TimeUnit.SECONDS);
                    log.info("Kafka 接入 topic 扩分区: {} → {}", topic, partitions);
                } catch (Exception e) {
                    if (i < 4) {
                        Thread.sleep(1000);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("确保 Kafka 接入 topic 失败(继续): {} - {}", topic, e.getMessage());
        }
    }

    private long lagOf(String group, String topic) {
        try (AdminClient admin = AdminClient.create(kafkaProperties.buildAdminProperties(null))) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                            .get(5, TimeUnit.SECONDS);
            if (committed == null || committed.isEmpty()) {
                return -1;
            }
            Map<TopicPartition, Long> ends = new HashMap<>();
            admin.listOffsets(committed.keySet().stream()
                            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                    .all().get(5, TimeUnit.SECONDS)
                    .forEach((tp, res) -> ends.put(tp, res.offset()));
            long lag = 0;
            boolean any = false;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : committed.entrySet()) {
                Long end = ends.get(e.getKey());
                if (end != null) {
                    lag += Math.max(0, end - e.getValue().offset());
                    any = true;
                }
            }
            return any ? lag : -1;
        } catch (Exception e) {
            return -1;
        }
    }

    private long appOk(String appKey) {
        Metrics m = metrics.get(appKey);
        return m == null ? 0 : m.ok;
    }

    private long dltCount(String appKey, String logicalTable) {
        if (logicalTable == null) {
            try {
                Long n = metaJdbc.queryForObject(
                        "SELECT COUNT(*) FROM dlt_message WHERE source_table = ?", Long.class, appKey);
                return n == null ? 0 : n;
            } catch (Exception e) {
                return 0;
            }
        }
        try {
            Long n = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM dlt_message WHERE source_table = ? AND logical_table = ?",
                    Long.class, appKey, logicalTable);
            return n == null ? 0 : n;
        } catch (Exception e) {
            return 0;
        }
    }

    // ────────────────────────── 内部结构 ──────────────────────────

    /** 鉴权失败(401 语义) */
    public static final class AuthException extends RuntimeException {
        public AuthException(String message) {
            super(message);
        }
    }

    /** 白名单/权限失败(403 语义) */
    public static final class ForbiddenException extends RuntimeException {
        public ForbiddenException(String message) {
            super(message);
        }
    }

    public static final class Analysis {
        public final String op;
        public final String businessKey;
        public final Map<String, Object> fields;
        public final String error;

        Analysis(String op, String businessKey, Map<String, Object> fields, String error) {
            this.op = op;
            this.businessKey = businessKey;
            this.fields = fields;
            this.error = error;
        }

        Analysis(String error) {
            this(null, null, null, error);
        }
    }

    private static final class Runtime {
        final ConcurrentMessageListenerContainer<String, String> container;

        Runtime(ConcurrentMessageListenerContainer<String, String> container) {
            this.container = container;
        }
    }

    private static final class Metrics {
        volatile long ok;
        volatile long dlt;
        private final Deque<long[]> ring = new ArrayDeque<>();

        synchronized void tick() {
            long sec = System.currentTimeMillis() / 1000;
            long[] last = ring.peekLast();
            if (last != null && last[0] == sec) {
                last[1] = ok;
                last[2] = dlt;
            } else {
                ring.addLast(new long[]{sec, ok, dlt});
                while (ring.size() > 61) {
                    ring.removeFirst();
                }
            }
        }

        synchronized List<Map<String, Object>> samples() {
            List<Map<String, Object>> out = new ArrayList<>();
            long prevOk = 0;
            long prevDlt = 0;
            for (long[] e : ring) {
                out.add(Map.of(
                        "t", LocalDateTime.ofInstant(Instant.ofEpochSecond(e[0]), ZoneId.systemDefault()).format(HM),
                        "ok", Math.max(0, e[1] - prevOk),
                        "dlt", Math.max(0, e[2] - prevDlt)));
                prevOk = e[1];
                prevDlt = e[2];
            }
            return out;
        }
    }
}

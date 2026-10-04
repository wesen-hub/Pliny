package com.dcp.sync.ingest;

import com.dcp.sync.LogicalTableConfigService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 接入引擎(2026-09-06 应用/凭证模型; 2026-10 P3 退役改造):
 *
 * <pre>
 *   页面: 新建「接入应用」→ 平台颁发 appKey(创建即启用, 白名单绑定逻辑表)
 *   对方: SDK/HTTP 携带 appKey 提交消息(op 在数据包内指定)
 *   HTTP: 401 key 无效/停用 → 403 未绑定该表 → 400 预检(op/字段/类型/业务键) → 200 入队
 *   dcp.in.{逻辑表}(削峰) → 底座任务(kafka-ingest@1.0.0, dcp-outlet-rt 执行)
 *     验证 → DLT(source_table=appKey) / 通过: 领 seq + 信封 → dcp.{逻辑表}
 * </pre>
 *
 * <p><b>退役改造</b>:入站消费段(容器/领号/打信封/提交)已迁出至底座任务,本引擎保留
 * HTTP/SDK 提交面(鉴权 + 白名单 + 预检 + 入站 topic 管理)与应用管理面;
 * 消费任务的部署登记/停用由 {@link IngestTaskDeployer} 桥接控制面(<b>只登记不激活</b>,
 * 激活留到部署方案阶段统一解决执行栈归属与解码器后 resume 复活)。
 * 监控指标口径随之切换:ok = seq_counter 累计发号(底座领号), lag/running = 底座任务进度/状态,
 * 吞吐采样环随消费段退役(前端显示"暂无流量")。
 */
@Slf4j
@Component
public class KafkaIngestEngine {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final SecureRandom RAND = new SecureRandom();
    public static final String IN_TOPIC_PREFIX = "dcp.in.";

    private final JdbcTemplate metaJdbc;
    private final KafkaProperties kafkaProperties;
    private final KafkaTemplate<String, String> kafka;
    private final LogicalTableConfigService tableConfigService;
    private final IngestTaskDeployer deployer;

    @Autowired
    public KafkaIngestEngine(JdbcTemplate metaJdbc, KafkaProperties kafkaProperties,
                             KafkaTemplate<String, String> kafka,
                             LogicalTableConfigService tableConfigService,
                             IngestTaskDeployer deployer) {
        this.metaJdbc = metaJdbc;
        this.kafkaProperties = kafkaProperties;
        this.kafka = kafka;
        this.tableConfigService = tableConfigService;
        this.deployer = deployer;
    }

    // ────────────────────────── 表结构与启动 ──────────────────────────

    @PostConstruct
    public void ensureTable() {
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
                ensureInTopic(lt);
                deployer.deploy(lt, partitionCountOf(lt));
            } catch (Exception e) {
                log.error("Kafka 接入底座任务登记失败: {} - {}", lt, e.getMessage());
            }
        }
        log.info("Kafka 接入引擎就绪(应用/凭证模型; 消费段已迁出至底座 kafka-ingest)");
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
            ensureInTopic(lt);
            deployer.deploy(lt, partitionCountOf(lt));
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
            row.put("ok", appOk(key, tables));
            row.put("dlt", dltCount(key, null));
            row.put("running", tables.stream().anyMatch(lt -> "RUNNING".equals(deployer.statusOf(lt))));
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
        List<Map<String, Object>> tables = new ArrayList<>();
        for (String lt : tablesOfApp(appId)) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("logicalTable", lt);
            t.put("ok", seqOf(lt, appKey));
            t.put("dlt", dltCount(appKey, lt));
            t.put("lag", deployer.lagOf(lt));
            t.put("running", "RUNNING".equals(deployer.statusOf(lt)));
            tables.add(t);
        }
        List<String> boundTables = tables.stream().map(t -> String.valueOf(t.get("logicalTable"))).toList();
        Map<String, Object> out = new LinkedHashMap<>(app);
        out.put("tables", tables);
        out.put("ok", appOk(appKey, boundTables));
        out.put("dlt", dltCount(appKey, null));
        out.put("samples", List.of());   // 吞吐采样环已随消费段退役(底座指标由 outlet 侧采集)
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

    /** 停用/删除/重绑后: 无启用应用的表停底座任务(保留登记), 有则确保已登记 */
    private void reconcile(List<String> tables) {
        for (String lt : new HashSet<>(tables)) {
            Long enabled = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM kafka_app_logical l JOIN kafka_app a ON a.id = l.app_id "
                            + "WHERE l.logical_table = ? AND a.status = 'ENABLED'", Long.class, lt);
            if (enabled == null || enabled == 0) {
                deployer.stop(lt);
            } else {
                try {
                    ensureInTopic(lt);
                    deployer.deploy(lt, partitionCountOf(lt));
                } catch (Exception e) {
                    log.warn("Kafka 接入底座任务登记失败: {} - {}", lt, e.getMessage());
                }
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

    // ────────────────────────── 验证器(HTTP 预检与底座消费端同源) ──────────────────────────

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

    // ────────────────────────── 入站消息 / topic / 指标口径 ──────────────────────────

    private String buildInbound(String op, String ts, String appKey, Map<String, Object> data) {
        ObjectNode node = OM.createObjectNode();
        node.put("op", op);
        node.put("ts", ts == null || ts.isBlank()
                ? LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : ts);
        node.put("submitter", appKey);
        node.set("data", OM.valueToTree(data));
        return node.toString();
    }

    /** 确保入站 topic 存在且分区数达标(提交面职责; 底座消费方假定 topic 已就绪) */
    private void ensureInTopic(String logicalTable) {
        String topic = IN_TOPIC_PREFIX + logicalTable;
        int partitions = partitionCountOf(logicalTable);
        try (AdminClient admin = AdminClient.create(kafkaProperties.buildAdminProperties(null))) {
            // describe 对不存在 topic 抛 UnknownTopicOrPartitionException(而非返回 null 条目),
            // 故异常一律按"不存在"处理直接创建, 创建竞态由 createTopics 侧告警吸收
            boolean exists = true;
            try {
                admin.describeTopics(List.of(topic)).allTopicNames()
                        .get(5, TimeUnit.SECONDS).get(topic);
            } catch (Exception e) {
                exists = false;
            }
            if (!exists) {
                try {
                    admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                            .all().get(30, TimeUnit.SECONDS);
                    log.info("Kafka 接入 topic 已创建: {} partitions={}", topic, partitions);
                } catch (Exception e) {
                    log.warn("Kafka 接入 topic 创建竞态(待校验): {} - {}", topic, e.getMessage());
                }
            }
            for (int i = 0; i < 5 && partitions > 1; i++) {
                int cur;
                try {
                    cur = admin.describeTopics(List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS)
                            .get(topic).partitions().size();
                } catch (Exception e) {
                    break;   // 元数据不可达 → 放弃扩分区(后续提交前会再兜底)
                }
                if (cur >= partitions) {
                    break;
                }
                try {
                    admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(partitions)))
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

    private int partitionCountOf(String logicalTable) {
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        return cfg.getPartitionCount() > 0 ? cfg.getPartitionCount() : 8;
    }

    /** 分表 ok 口径: 底座消费领号(seq_counter)累计数 = 已消费行数 */
    private long seqOf(String logicalTable, String appKey) {
        try {
            Long cur = metaJdbc.queryForObject("SELECT cur FROM seq_counter WHERE table_name = ?",
                    Long.class, "kafka|" + logicalTable + "|" + appKey);
            return cur == null ? 0 : cur;
        } catch (Exception e) {
            return 0;
        }
    }

    private long appOk(String appKey, List<String> tables) {
        long sum = 0;
        for (String lt : tables) {
            sum += seqOf(lt, appKey);
        }
        return sum;
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
}

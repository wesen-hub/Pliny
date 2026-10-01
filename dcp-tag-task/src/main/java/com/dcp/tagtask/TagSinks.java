package com.dcp.tagtask;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 多写 sink(语义对齐 dcp-tagstream.StreamSinks, 非 Flink 形态):
 * Redis 在线层(tag:online:{entity} hash + TTL) + MySQL 投影层(tag_fact_view + tag_notify)
 * + Kafka 打标事件流(dcp.tag.events, tag 契约订阅推送底座任务 dcp-sub-push 的数据源)。
 *
 * <p><b>失败策略(底座契约"失败要抛")</b>:
 * <ul>
 *   <li>在线层(hset/expire/hdel)失败 → 抛异常, 平台不提交→重试→死信;</li>
 *   <li>投影层(tag_fact_view upsert/delete)失败 → 抛异常;</li>
 *   <li>事件流发射失败 → 抛异常(与投影层同级: 事件是订阅推送的数据正确性来源,
 *       重发幂等, 订阅方至少一次);</li>
 *   <li>tag_notify 审计写失败 → 仅告警不阻断(与 StreamSinks 一致: 审计不阻断主链路)。</li>
 * </ul>
 */
public final class TagSinks {

    private static final Logger log = LoggerFactory.getLogger(TagSinks.class);

    private TagSinks() {
    }

    private static final ObjectMapper OM = new ObjectMapper();

    /** 在线层写入接口(单测可 fake; 主链路失败抛异常) */
    public interface TagOnline {
        void put(String entity, Map<String, String> tags);

        void remove(String entity);

        void close();
    }

    /** 投影层写入接口(单测可 fake; 主链路失败抛异常) */
    public interface TagView {
        void put(String entity, Map<String, String> tags, long ts) throws Exception;

        void remove(String entity) throws Exception;

        void close();
    }

    /** Redis 在线层: hit 事件 hset+expire; 行删除(事件 DELETE)时 hdel 整实体 */
    public static final class OnlineSink implements TagOnline {
        private final TagParams cfg;
        private final Jedis jedis;

        public OnlineSink(TagParams cfg) {
            this.cfg = cfg;
            this.jedis = new Jedis(cfg.online.redisHost, cfg.online.redisPort);
        }

        /** 命中双写在线层(hset + 续 TTL) */
        public void put(String entity, Map<String, String> tags) {
            String key = "tag:online:" + entity;
            jedis.hset(key, tags);
            jedis.expire(key, cfg.online.ttlSeconds);
        }

        /** 事件 DELETE: 清该实体全部在线标签 */
        public void remove(String entity) {
            jedis.del("tag:online:" + entity);
        }

        public void close() {
            jedis.close();
        }
    }

    /** MySQL 投影层(tag_fact_view / tag_notify; 与批投影同表同结构) */
    public static final class ViewSink implements TagView {
        private final TagParams cfg;
        private final Connection conn;

        public ViewSink(TagParams cfg) throws Exception {
            this.cfg = cfg;
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.conn = DriverManager.getConnection(cfg.factView.jdbcUrl, cfg.factView.user, cfg.factView.password);
            try (java.sql.Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE IF NOT EXISTS `" + cfg.factView.table + "` ("
                        + "task_id BIGINT NOT NULL, entity VARCHAR(191) NOT NULL, tag VARCHAR(191) NOT NULL,"
                        + "tag_dict_id BIGINT NULL,"
                        + "value VARCHAR(255) NULL, run_ts DATETIME NULL,"
                        + "PRIMARY KEY (task_id, entity, tag))");
                // P1.2④: 标签变更审计(订阅消费端); 与 backend/executor 同一 tag_notify 契约
                st.execute("CREATE TABLE IF NOT EXISTS tag_notify ("
                        + "entity VARCHAR(128) NOT NULL, tag_path VARCHAR(512) NOT NULL,"
                        + "tag_dict_id BIGINT NULL,"
                        + "tag_value VARCHAR(128) NULL, task_id BIGINT NOT NULL,"
                        + "task_mode VARCHAR(16) NOT NULL,"
                        + "occur_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
                        + "PRIMARY KEY (entity, tag_path, task_id))");
            }
            // 已存在的表补列(幂等)
            addColumnIfMissing("`" + cfg.factView.table + "`", "tag_dict_id", "BIGINT NULL");
            addColumnIfMissing("tag_notify", "tag_dict_id", "BIGINT NULL");
        }

        private void addColumnIfMissing(String table, String col, String type) {
            try (java.sql.Statement st = conn.createStatement()) {
                st.execute("ALTER TABLE " + table + " ADD COLUMN " + col + " " + type);
            } catch (Exception ignored) {
                // 列已存在
            }
        }

        /** 命中写投影 + 审计(task_mode='stream', 与切流前一致) */
        public void put(String entity, Map<String, String> tags, long ts) throws Exception {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO `" + cfg.factView.table + "`(task_id, entity, tag, tag_dict_id, value, run_ts)"
                            + " VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE tag_dict_id=VALUES(tag_dict_id), value=VALUES(value), run_ts=VALUES(run_ts)")) {
                for (Map.Entry<String, String> e : tags.entrySet()) {
                    ps.setLong(1, cfg.taskId);
                    ps.setString(2, entity);
                    ps.setString(3, e.getKey());
                    Long tid = cfg.tagIds == null ? null : cfg.tagIds.get(e.getKey());
                    if (tid == null) ps.setNull(4, java.sql.Types.BIGINT); else ps.setLong(4, tid);
                    ps.setString(5, e.getValue() == null || e.getValue().isEmpty() ? null : e.getValue());
                    ps.setTimestamp(6, new java.sql.Timestamp(ts));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try {
                notify(entity, tags);
            } catch (Exception e) {
                // 审计不阻断主链路(与 StreamSinks 一致)
                log.warn("[tag-task] tag_notify 审计写失败(entity={}): {}", entity, e.getMessage());
            }
        }

        /** 事件 DELETE: 删投影行(审计保留历史) */
        public void remove(String entity) throws Exception {
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM `" + cfg.factView.table + "` WHERE task_id=? AND entity=?")) {
                ps.setLong(1, cfg.taskId);
                ps.setString(2, entity);
                ps.executeUpdate();
            }
        }

        private void notify(String entity, Map<String, String> tags) throws Exception {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO tag_notify(entity, tag_path, tag_dict_id, tag_value, task_id, task_mode)"
                            + " VALUES(?,?,?,?,?,'stream')"
                            + " ON DUPLICATE KEY UPDATE tag_value=VALUES(tag_value), tag_dict_id=VALUES(tag_dict_id), task_mode='stream', occur_ts=CURRENT_TIMESTAMP(3)")) {
                for (Map.Entry<String, String> e : tags.entrySet()) {
                    ps.setString(1, entity);
                    ps.setString(2, e.getKey());
                    Long tid = cfg.tagIds == null ? null : cfg.tagIds.get(e.getKey());
                    if (tid == null) ps.setNull(3, java.sql.Types.BIGINT); else ps.setLong(3, tid);
                    ps.setString(4, e.getValue() == null || e.getValue().isEmpty() ? null : e.getValue());
                    ps.setLong(5, cfg.taskId);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }

        public void close() {
            try {
                conn.close();
            } catch (Exception ignored) {
            }
        }
    }

    /** 打标事件流发射接口(单测可 fake; 主链路失败抛异常) */
    public interface TagEvents {
        void emit(String entity, Map<String, String> tags, long ts) throws Exception;

        void close();
    }

    /**
     * Kafka 打标事件流发射(topic 缺省 dcp.tag.events; 订阅推送底座任务 sub-push 消费同 topic):
     * 逐标签发 {@code {taskId, entity, tagPath, tagDictId, tagValue, ts(epoch millis)}},
     * 同步发送(at-least-once, 失败抛 → 平台重试 → 事件重复, 订阅方至少一次语义)。
     */
    public static final class KafkaTagEvents implements TagEvents {
        private final TagParams cfg;
        private final String topic;
        private final KafkaProducer<String, String> producer;

        public KafkaTagEvents(TagParams cfg) {
            this.cfg = cfg;
            this.topic = cfg.tagEvents.topic == null || cfg.tagEvents.topic.isBlank()
                    ? "dcp.tag.events" : cfg.tagEvents.topic;
            Properties p = new Properties();
            p.put("bootstrap.servers", cfg.tagEvents.kafkaBootstrap == null || cfg.tagEvents.kafkaBootstrap.isBlank()
                    ? "kafka:9092" : cfg.tagEvents.kafkaBootstrap);
            p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            p.put("acks", "all");
            this.producer = new KafkaProducer<>(p);
        }

        @Override
        public void emit(String entity, Map<String, String> tags, long ts) throws Exception {
            for (Map.Entry<String, String> e : tags.entrySet()) {
                ObjectNode ev = OM.createObjectNode();
                ev.put("taskId", cfg.taskId);
                ev.put("entity", entity);
                ev.put("tagPath", e.getKey());
                Long tid = cfg.tagIds == null ? null : cfg.tagIds.get(e.getKey());
                if (tid != null) ev.put("tagDictId", tid);
                String v = e.getValue() == null || e.getValue().isEmpty() ? null : e.getValue();
                if (v != null) ev.put("tagValue", v);
                ev.put("ts", ts);
                producer.send(new ProducerRecord<>(topic, OM.writeValueAsString(ev)))
                        .get(5, TimeUnit.SECONDS);
            }
        }

        @Override
        public void close() {
            producer.close();
        }
    }
}

package com.dcp.tagstream;

import com.dcp.tagstream.config.StreamConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

/** 双写 sink: Redis tag:online:{entity}(hash tag→value + TTL) + MySQL tag_fact_view(投影事实层) */
public final class StreamSinks {

    private StreamSinks() {
    }

    /** Redis 在线层 sink */
    public static final class RedisSink extends RichSinkFunction<StreamEvaluator.Hit> {
        private static final long serialVersionUID = 1L;
        private final StreamConfig.OnlineRef conf;
        private transient Jedis jedis;

        public RedisSink(StreamConfig.OnlineRef conf) {
            this.conf = conf;
        }

        @Override
        public void open(Configuration parameters) {
            jedis = new Jedis(conf.redisHost, conf.redisPort);
        }

        @Override
        public void invoke(StreamEvaluator.Hit hit, Context ctx) {
            String key = "tag:online:" + hit.entity;
            jedis.hset(key, hit.tags);
            jedis.expire(key, conf.ttlSeconds);
        }

        @Override
        public void close() {
            if (jedis != null) jedis.close();
        }
    }

    /** MySQL/TiDB 投影 sink(tag_fact_view; 与批投影同表同结构) */
    public static final class ViewSink extends RichSinkFunction<StreamEvaluator.Hit> {
        private static final long serialVersionUID = 1L;
        private final StreamConfig cfg;
        private transient Connection conn;

        public ViewSink(StreamConfig cfg) {
            this.cfg = cfg;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(cfg.factView.jdbcUrl, cfg.factView.user, cfg.factView.password);
            try (java.sql.Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE IF NOT EXISTS `" + cfg.factView.table + "` ("
                        + "task_id BIGINT NOT NULL, entity VARCHAR(191) NOT NULL, tag VARCHAR(191) NOT NULL,"
                        + "value VARCHAR(255) NULL, run_ts DATETIME NULL,"
                        + "PRIMARY KEY (task_id, entity, tag))");
            }
        }

        @Override
        public void invoke(StreamEvaluator.Hit hit, Context ctx) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO `" + cfg.factView.table + "`(task_id, entity, tag, value, run_ts)"
                            + " VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE value=VALUES(value), run_ts=VALUES(run_ts)")) {
                for (Map.Entry<String, String> e : hit.tags.entrySet()) {
                    ps.setLong(1, cfg.taskId);
                    ps.setString(2, hit.entity);
                    ps.setString(3, e.getKey());
                    ps.setString(4, e.getValue() == null || e.getValue().isEmpty() ? null : e.getValue());
                    ps.setTimestamp(5, new java.sql.Timestamp(hit.ts));
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (Exception e) {
                System.err.println("[tagstream] 投影写失败: " + e.getMessage());
            }
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
        }
    }
}

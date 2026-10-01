package com.dcp.filesync.core;

import com.dcp.filesync.util.CryptoUtil;
import com.dcp.filesync.util.Strategy;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 文件同步任务定义读取(执行器侧): 从元库读 file_sync_task + datasource_config,
 * 解析源/目标连接(含凭证解密), 组装执行所需全量上下文。
 *
 * <p>凭证安全(方案 §7.4/§9 提交链路的自然延伸): backend 提交 TaskSpec 的 bizMeta 只放
 * {taskId, runId}, 凭证密文留在元库、密钥经 env(dcp.filesync.enc-key) 注入——凭证不进 etcd。
 * 与 N4 LogicalTableReader(读元库分表配置)同一模式: 业务 jar 经 env.metaDataSource() 读元库。
 */
public class FilesyncTaskReader {

    private static final Logger log = LoggerFactory.getLogger(FilesyncTaskReader.class);

    private final DataSource ds;
    private final String encKey;
    /** 平台默认目标 MinIO(任务 target_datasource_id=0 时使用; 对应 backend dcp.filecopy.target-*) */
    private final String dstEndpoint;
    private final String dstAccessKey;
    private final String dstSecretKey;

    public FilesyncTaskReader(DataSource ds, String encKey,
                              String dstEndpoint, String dstAccessKey, String dstSecretKey) {
        this.ds = ds;
        this.encKey = encKey;
        this.dstEndpoint = dstEndpoint;
        this.dstAccessKey = dstAccessKey;
        this.dstSecretKey = dstSecretKey;
    }

    /** 加载任务定义(不存在/非 platform 返回 null 由调用方决定收尾方式)。 */
    public FilesyncTask load(long taskId) {
        String sql = "SELECT t.id, t.exec_host, t.source_datasource_id, t.source_path, "
                + "t.target_datasource_id, t.target_bucket, t.target_prefix, t.strategy_json, "
                + "t.sync_mode, t.baseline_done FROM file_sync_task t WHERE t.id = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    log.warn("[filesync] 任务不存在 task={}", taskId);
                    return null;
                }
                FilesyncTask t = new FilesyncTask();
                t.taskId = rs.getLong("id");
                t.execHost = rs.getString("exec_host");
                t.sourcePath = rs.getString("source_path");
                t.targetBucket = rs.getString("target_bucket");
                t.targetPrefix = rs.getString("target_prefix");
                t.strategyJson = rs.getString("strategy_json");
                t.syncMode = Strategy.normMode(rs.getString("sync_mode"));
                t.baselineDone = rs.getBoolean("baseline_done");
                long srcDs = rs.getLong("source_datasource_id");
                long dstDs = rs.getLong("target_datasource_id");
                t.source = sourceConf(srcDs);
                t.source.put("path", t.sourcePath == null ? "" : t.sourcePath);
                t.target = targetConf(dstDs, t.targetBucket, t.targetPrefix);
                return t;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取文件同步任务失败: " + taskId, e);
        }
    }

    /** 源侧连接解析(s3 命名 remote / local 绝对路径; 迁自 backend sourceConf) */
    private Map<String, String> sourceConf(long datasourceId) {
        Map<String, Object> r = queryDatasource(datasourceId);
        String type = String.valueOf(r.get("type"));
        ObjectNode node = Strategy.extraNode(r.get("extra_params"));
        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("type", type);
        if ("local".equals(type)) {
            String root = node.path("root").asText("/data");
            conf.put("root", root);
            conf.put("endpoint", "");
            conf.put("accessKey", "");
            conf.put("secretKey", "");
        } else if ("s3".equals(type)) {
            String ak = String.valueOf(r.get("username"));
            String sk = CryptoUtil.decrypt(String.valueOf(r.get("password_ciphertext")), encKey);
            conf.put("endpoint", "http://" + r.get("host") + ":" + r.get("port"));
            conf.put("accessKey", ak);
            conf.put("secretKey", sk);
            conf.put("bucket", node.path("bucket").asText(
                    r.get("database_name") == null ? "" : String.valueOf(r.get("database_name"))));
        } else {
            throw new IllegalStateException("文件同步源仅支持 s3/local 数据源: " + type);
        }
        return conf;
    }

    /** 目标侧连接解析(显式 s3 数据源 / 平台默认 MinIO; 迁自 backend targetConf) */
    private Map<String, String> targetConf(long dsId, String bucket, String prefix) {
        Map<String, String> conf = new LinkedHashMap<>();
        if (dsId <= 0) {
            conf.put("endpoint", dstEndpoint);
            conf.put("accessKey", dstAccessKey);
            conf.put("secretKey", dstSecretKey);
        } else {
            Map<String, Object> r = queryDatasource(dsId);
            if (r.isEmpty() || !"s3".equals(String.valueOf(r.get("type")))) {
                throw new IllegalStateException("目标数据源不存在或非 s3: " + dsId);
            }
            conf.put("endpoint", "http://" + r.get("host") + ":" + r.get("port"));
            conf.put("accessKey", String.valueOf(r.get("username")));
            conf.put("secretKey", CryptoUtil.decrypt(String.valueOf(r.get("password_ciphertext")), encKey));
        }
        conf.put("bucket", bucket);
        conf.put("prefix", prefix == null ? "" : Strategy.strip(prefix));
        return conf;
    }

    private Map<String, Object> queryDatasource(long id) {
        String sql = "SELECT type, host, port, database_name, username, password_ciphertext, extra_params "
                + "FROM datasource_config WHERE id = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Map.of();
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("type", rs.getString("type"));
                r.put("host", rs.getString("host"));
                r.put("port", rs.getInt("port"));
                r.put("database_name", rs.getString("database_name"));
                r.put("username", rs.getString("username"));
                r.put("password_ciphertext", rs.getString("password_ciphertext"));
                r.put("extra_params", rs.getString("extra_params"));
                return r;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取数据源失败: " + id, e);
        }
    }

    /** 执行上下文(纯数据载体)。 */
    public static final class FilesyncTask {
        public long taskId;
        public String execHost;
        public String sourcePath;
        public String targetBucket;
        public String targetPrefix;
        public String strategyJson;
        public String syncMode;
        public boolean baselineDone;
        public Map<String, String> source;
        public Map<String, String> target;

        public Map<String, Object> strategy() {
            return Strategy.parseJson(strategyJson);
        }

        public boolean isPlatform() {
            return !"agent".equals(execHost);
        }
    }
}

package com.dcp.lakesync.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 湖同步作业配置(--job-json, 平台侧落盘; 结构对应设计文档 §3/§4b)。
 *
 * <pre>
 * {
 *   "logicalTable": "t_user_model",
 *   "entityKeyColumns": ["user_id"],
 *   "dataSource": { "url": "jdbc:mysql://.../target_db?...", "user": "root", "password": "...", "table": "t_user_model" },
 *   "seqColumns": [ {"name": "_seq_a", "cursor": 0}, {"name": "_seq_b", "cursor": 0} ],
 *   "tombstone": { "enabled": true, "column": "_deleted" },     // 可选; 默认透传全部业务列
 *   "lake": {
 *      "catalog": "demo", "namespace": "dcp", "table": "t_user_model",
 *      "type": "rest", "uri": "http://dcp-polaris:8181/api/catalog",
 *      "warehouse": "demo",           // REST(Polaris): catalog 名; hadoop 型: s3://…/基路径
 *      "token": "…",                  // 或 credential/scope/oauth2ServerUri
 *      "credential": "id:secret", "scope": "PRINCIPAL_ROLE:ALL", "oauth2ServerUri": null,
 *      "ioImpl": "org.apache.iceberg.aws.s3.S3FileIO",
 *      "s3": { "endpoint": "http://dcp-minio:9000", "pathStyleAccess": true,
 *               "accessKey": "…", "secretKey": "…" }
 *   }
 * }
 * </pre>
 */
public class LakeJobConfig {

    private static final ObjectMapper OM = new ObjectMapper();

    public String logicalTable;
    public List<String> entityKeyColumns = new ArrayList<>();
    public DataSource dataSource = new DataSource();
    public List<SeqColumn> seqColumns = new ArrayList<>();
    public Tombstone tombstone = new Tombstone();
    public Lake lake = new Lake();

    public static class DataSource {
        public String url;
        public String user;
        public String password;
        public String table;
    }

    /** 源表每源 seq 列(消费端多源合并行, 各源独立列); cursor=平台侧游标初值(-1 = 该源全量首载) */
    public static class SeqColumn {
        public String name;
        public long cursor = -1;
    }

    public static class Tombstone {
        public boolean enabled;
        public String column = "_deleted";
    }

    public static class Lake {
        public String catalog = "demo";
        public String namespace;
        public String table;
        public String type = "rest";
        public String uri;
        public String warehouse;                 // REST: catalog 名; hadoop: s3://基路径
        public String token;
        public String credential;
        public String scope;
        public String oauth2ServerUri;
        public String ioImpl = "org.apache.iceberg.aws.s3.S3FileIO";
        public Map<String, String> s3;           // endpoint/pathStyleAccess/accessKey/secretKey/region
    }

    /** 源侧增量拉取谓词: 任一源 seq 列 > 对应游标(OR);
     * 若任一列 cursor<0(该列从未同步/新写者列加入)→ 整轮退化全量(NULL),
     * 否则只按已同步列增量会漏掉未同步列的历史数据 */
    public String pullPredicate() {
        if (seqColumns.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (SeqColumn sc : seqColumns) {
            if (sc.cursor < 0) {
                return null;   // 任一列未同步 → 全量首载/重载(MERGE 幂等)
            }
            if (sb.length() > 0) sb.append(" OR ");
            sb.append('`').append(sc.name).append("` > ").append(sc.cursor);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    public static LakeJobConfig load(String path) {
        try {
            String json = Files.readString(Paths.get(path), StandardCharsets.UTF_8);
            LakeJobConfig cfg = OM.readValue(json, LakeJobConfig.class);
            validate(cfg);
            return cfg;
        } catch (IOException e) {
            throw new IllegalArgumentException("job-json 读取/解析失败: " + e.getMessage());
        }
    }

    private static void validate(LakeJobConfig c) {
        if (c.logicalTable == null || c.logicalTable.isBlank()) throw new IllegalArgumentException("logicalTable 必填");
        if (c.dataSource == null || isBlank(c.dataSource.url) || isBlank(c.dataSource.table)) {
            throw new IllegalArgumentException("dataSource.url/table 必填");
        }
        if (c.lake == null || isBlank(c.lake.catalog) || isBlank(c.lake.namespace) || isBlank(c.lake.table)) {
            throw new IllegalArgumentException("lake.catalog/namespace/table 必填");
        }
        if (c.lake.uri == null || c.lake.uri.isBlank()) throw new IllegalArgumentException("lake.uri(REST)必填");
        if (c.entityKeyColumns == null || c.entityKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("entityKeyColumns 必填(湖 MERGE 主键)");
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}

package com.dcp.laketask;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 底座任务 params(backend LakeSyncService 提交任务时组装; 与批作业 job-json 的 lake 段同源同键纪律)。
 *
 * <pre>
 * logicalTable   逻辑表(消费 dcp.{逻辑表} 信封流; 湖表名缺省同 logicalTable)
 * jdbcUrl/user/password  元库连接(logical_table 业务键读 / dlt_message 落库)
 * entityKeyColumns  湖表业务键列(可选; 缺省从元库 logical_table.key_columns 读)
 * lake.catalog/namespace/table/type/uri/warehouse/token/credential/scope/oauth2ServerUri/ioImpl/s3
 *                  —— 与 dcp-lakesync job-json lake 段完全同构(同一湖表同一 catalog);
 *                     credential/scope/oauth2ServerUri 为长驻进程推荐形态(RESTCatalog 自动换 token),
 *                     token 为短时形态(批作业 fetchToken 同款, 过期需重启单元)
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LakeParams {

    public String logicalTable;
    public String jdbcUrl;
    public String user;
    public String password;
    public List<String> entityKeyColumns = new ArrayList<>();
    public Lake lake = new Lake();

    /** 湖目标配置(与 LakeJobConfig.Lake 同结构: 批作业与增量任务共用同一湖表) */
    @JsonIgnoreProperties(ignoreUnknown = true)
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
}

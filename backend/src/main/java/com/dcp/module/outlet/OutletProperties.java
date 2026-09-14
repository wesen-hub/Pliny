package com.dcp.module.outlet;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 出口服务配置(前缀 {@code dcp.outlet})。
 *
 * <pre>
 * dcp.outlet:
 *   source: mock            # mock | kafka
 *   registry: memory        # memory | mapper
 *   epoch: 1                # 流代次(重建 changefeed / topic 重建时 +1)
 *   kafka:
 *     topics: dcp.cdc.target_db
 *     group-id: dcp-outlet
 *     start-tso: 0          # 流的起点(= changefeed.start_ts);earliest 不允许小于它
 *     buffer-max-events: 20000
 *   tidb:
 *     url: jdbc:mysql://dcp-outlet-tidb1:4000/target_db
 *     username: root
 *     password: ***
 *     pk-column: id
 * </pre>
 */
@Data
@Component
@ConfigurationProperties(prefix = "dcp.outlet")
public class OutletProperties {

    /** 数据源:mock(骨架) | kafka(L1 内部流) */
    private String source = "mock";
    /** 注册表:memory(骨架) | mapper(元库) */
    private String registry = "memory";
    /** 流代次 */
    private int epoch = 1;
    /** 保留窗口(小时,决策 12:≥168) */
    private int retentionHours = 168;

    private Kafka kafka = new Kafka();
    private Tidb tidb = new Tidb();
    private Snapshot snapshot = new Snapshot();
    private Api api = new Api();
    private Spark spark = new Spark();
    private Monitor monitor = new Monitor();

    @Data
    public static class Kafka {
        private String topics = "dcp.cdc.target_db";
        private String groupId = "dcp-outlet";
        /** 流的起点(= changefeed.start_ts);earliest 不得小于它 */
        private long startTso = 0L;
        private int bufferMaxEvents = 20000;
    }

    @Data
    public static class Api {
        /** 出口 L3 客户端模式:inprocess(默认,内部消费方直调) | http */
        private String mode = "inprocess";
        /** http 模式下的出口服务地址(如 http://localhost:8080) */
        private String baseUrl = "http://localhost:8080";
        /** 进程内模式的默认消费方身份(HTTP 模式由调用方通过 token 携带) */
        private String consumerId = "demo-tag";
    }

    @Data
    public static class Spark {
        private String sparkSubmit = "spark-submit";
        private String master = "yarn";
        private String jobJar = "/opt/dcp/outlet-batch-job.jar";
        private String executorCores = "2";
        private String executorMemory = "2g";
        private String queue = "dcp";
        /** 不可变 staging 根目录(生产:Iceberg 仓库或对象存储) */
        private String stagingRoot = "s3a://dcp-staging/outlet";
        /** true = 只生成规格不真提交(无 Spark 环境时的安全默认) */
        private boolean dryRun = true;
    }

    @Data
    public static class Snapshot {        /** 快照任务 TTL(小时)。默认 24h;必须 ≤ GC 窗口,否则同一 boundary 不可再续 */
        private int ttlHours = 24;
        /** 单页行数上限 */
        private int pageLimit = 1000;
    }

    @Data
    public static class Tidb {        /** 快照只读入口(生产:契约化只读视图/账号,见设计 §6.3 技术要点 3) */
        private String url;
        private String username;
        private String password;
        /** 游标分页主键列(绝不用 LIMIT/OFFSET) */
        private String pkColumn = "id";
    }

    /**
     * 出口监控配置(前缀 {@code dcp.outlet.monitor})。
     *
     * <p>阈值口径与 {@code outlet/monitor-outlet-stack.sh} 和 Prometheus 告警规则**必须一致**:
     * LAG_WARN=300s / LAG_CRIT=3600s / GC_WARN=21600s / ISR&lt;RF。
     */
    @Data
    public static class Monitor {
        private boolean enabled = true;
        /** 采集间隔(ms,建议 15000~30000) */
        private long intervalMs = 20000L;
        private long initialDelayMs = 20000L;
        /** 被观测的 changefeed(topic 与之一一对应) */
        private String changefeedId = "chg-target-db";
        /** TiCDC OpenAPI 基地址(集群内:http://ticdc1:8300) */
        private String ticdcUrl = "http://ticdc1:8300";
        /** 被观测的 topic */
        private String topic = "dcp.cdc.target_db";
        /** Kafka 引导地址;为空则回退 spring.kafka.bootstrap-servers */
        private String kafkaBootstrap = "";
        /** changefeed checkpoint 滞后 WARN/CRIT(秒) */
        private long lagWarnSeconds = 300L;
        private long lagCritSeconds = 3600L;
        /** GC 剩余窗口 WARN(秒) */
        private long gcWarnSeconds = 21600L;
        /** 批作业失败率告警窗口(分钟) */
        private int batchFailWindowMinutes = 10;
    }
}

package com.dcp.indexer.core;

import com.dcp.scheduler.exec.ExecutorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.function.BiConsumer;

/**
 * 锚点采样组件组装器:业务 jar 无 Spring 容器,这里把整条链手工接起来。
 *
 * <pre>
 *   env.metaDataSource() → JdbcAnchorStore / JdbcWatermarkStore / JdbcPoisonStore
 *   → AnchorIndexer(policy + guard + 告警出口) —— 与迁出前 OutletConfig#anchorIndexer 同构,
 *   差异仅两点:① 告警出口无平台 AlertSink,落 slf4j 日志(与执行器 alert 可空语义一致);
 *             ② 配置键收敛为 dcp.indexer.*(由宿主 ExecutorEnvironment 提供)。
 * </pre>
 */
public class IndexerComponents {

    private static final Logger log = LoggerFactory.getLogger("outlet-indexer");

    private final String kafkaBootstrap;
    private final AnchorIndexer indexer;
    private final int epoch;
    private final String groupId;
    private final boolean fromBeginning;
    private final BiConsumer<String, String> alert;

    public IndexerComponents(ExecutorEnvironment env) {
        this.kafkaBootstrap = env.kafkaBootstrap("indexer");
        DataSource ds = env.metaDataSource();
        this.epoch = num(env, "dcp.indexer.epoch", 1);
        this.groupId = str(env, "dcp.indexer.group-id", "dcp-outlet-indexer");
        this.fromBeginning = bool(env, "dcp.indexer.from-beginning", false);
        // 索引器告警出口:jar 内无平台告警通道,落日志(生产可后续经 env 扩展接线)
        this.alert = (k, msg) -> log.warn("{}: {}", k, msg);
        this.indexer = new AnchorIndexer(
                new JdbcAnchorStore(ds), new JdbcWatermarkStore(ds),
                AnchorIndexPolicy.defaults(), new MonotonicityGuard(),
                System::currentTimeMillis, alert)
                .withPoisonStore(new JdbcPoisonStore(ds), epoch);
    }

    public String kafkaBootstrap() { return kafkaBootstrap; }

    public AnchorIndexer indexer() { return indexer; }

    public int epoch() { return epoch; }

    public String groupId() { return groupId; }

    public boolean fromBeginning() { return fromBeginning; }

    public BiConsumer<String, String> alert() { return alert; }

    private static String str(ExecutorEnvironment env, String key, String def) {
        String v = env.env().getOrDefault(key, def);
        return v == null ? def : v;
    }

    private static int num(ExecutorEnvironment env, String key, int def) {
        try {
            return Integer.parseInt(str(env, key, String.valueOf(def)).trim());
        } catch (NumberFormatException e) {
            log.warn("配置 {} 非法,回退默认 {}: {}", key, def, env.env().get(key));
            return def;
        }
    }

    private static boolean bool(ExecutorEnvironment env, String key, boolean def) {
        return Boolean.parseBoolean(str(env, key, String.valueOf(def)));
    }
}

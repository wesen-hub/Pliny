package com.dcp.filesync.exec;

import com.dcp.filesync.core.FilesyncTaskReader;
import com.dcp.filesync.core.RcloneEngine;
import com.dcp.filesync.core.RunRecorder;
import com.dcp.scheduler.exec.ExecutorEnvironment;
import com.dcp.scheduler.exec.ExecutorPlugin;
import com.dcp.scheduler.exec.PartitionExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * filesync-runner 执行器工厂(一级 SPI, 方案 N14): 宿主 dcp-worker 经 META-INF/services 发现本工厂,
 * 装配期调用一次 {@link #init(ExecutorEnvironment)} 构建共享组件(reader/recorder/engine),
 * 每分区 {@link #create()} 一次执行器。
 *
 * <p>配置键(dcp.filesync.*, 由宿主 ExecutorEnvironment.env() 注入):
 *   enc-key(凭证解密密钥, 与 backend dcp.enc-key 同值)、target.endpoint/access-key/secret-key
 *   (平台默认目标 MinIO, 对应 backend dcp.filecopy.target-*)。
 */
@ExecutorPlugin("filesync-runner")
public class FileSyncFactory implements PartitionExecutor.Factory {

    private static final Logger log = LoggerFactory.getLogger(FileSyncFactory.class);

    private volatile FilesyncTaskReader reader;
    private volatile RunRecorder recorder;
    private volatile RcloneEngine engine;

    @Override
    public synchronized void init(ExecutorEnvironment env) {
        if (reader == null) {
            DataSource ds = env.metaDataSource();
            if (ds == null) {
                throw new IllegalStateException("[filesync-runner] 宿主未提供 metaDataSource(dcp.worker.meta.jdbc-url)");
            }
            String encKey = str(env, "dcp.filesync.enc-key", "");
            String dstEndpoint = str(env, "dcp.filesync.target.endpoint", "http://host.docker.internal:19000");
            String dstAk = str(env, "dcp.filesync.target.access-key", "minioadmin");
            String dstSk = str(env, "dcp.filesync.target.secret-key", "minioadmin123");
            reader = new FilesyncTaskReader(ds, encKey, dstEndpoint, dstAk, dstSk);
            recorder = new RunRecorder(ds);
            engine = new RcloneEngine();
            log.info("[filesync-runner] 组件装配完成(enc-key 注入={} 默认目标={})", !encKey.isBlank(), dstEndpoint);
        }
    }

    @Override
    public PartitionExecutor create() {
        FilesyncTaskReader r = reader;
        if (r == null) {
            throw new IllegalStateException("FileSyncFactory 未初始化(Factory.init 未调用)");
        }
        return new FilesyncExecutor(r, recorder, engine);
    }

    private static String str(ExecutorEnvironment env, String key, String def) {
        String v = env.env().getOrDefault(key, def);
        return v == null ? def : v;
    }
}

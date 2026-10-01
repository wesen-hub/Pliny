package com.dcp.outlet.service.schedulerbridge;

import com.dcp.outlet.adapters.KafkaAdminStreamScanner;
import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;
import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.TaskSpec;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;

/**
 * 统一调度框架提交端(ns=indexer,方案 C2 ① 终态):outlet-service 不再内嵌调度实例、不直连 etcd,
 * 流扫描 → TaskSpec 经 dcp-worker REST 提交,anchor-sampler 执行器由 worker 装载 dcp-indexer-rt 承接。
 *
 * <pre>
 *   外壳职责:① 流扫描(KafkaAdminStreamScanner)→ 每个流提交 TaskSpec ② 启动重建(对账)。
 *   bizMeta(epoch/groupId/fromBeginning)来自 IndexerExecutorEnvironment 配置源(与业务 jar 约定键一致)。
 * </pre>
 */
@Component
public class IndexerSchedulerRuntime {

    private static final Logger log = LoggerFactory.getLogger(IndexerSchedulerRuntime.class);

    private final IndexerExecutorEnvironment executorEnvironment;
    private final StreamRegistry registry;
    private final KafkaAdminStreamScanner scanner;
    private final WorkerClient worker;

    public IndexerSchedulerRuntime(IndexerExecutorEnvironment executorEnvironment,
                                   StreamRegistry registry,
                                   KafkaAdminStreamScanner scanner,
                                   WorkerClient worker) {
        this.executorEnvironment = executorEnvironment;
        this.registry = registry;
        this.scanner = scanner;
        this.worker = worker;
    }

    @PostConstruct
    public void start() {
        // 外壳职责:启动重建(流登记表 → 全部 TaskSpec;框架从 etcd 自愈,不依赖外壳进程常驻)
        resubmitAll();
        log.info("indexer 提交端就绪: ns=indexer 任务提交走 dcp-worker REST, kafka={}",
                executorEnvironment.kafkaBootstrap("indexer"));
    }

    /** 外壳提交:流登记(新表感知)→ TaskSpec(接入设计 §五 ②;重复提交 = 更新)。 */
    public void submitIndexTask(StreamInfo info) {
        TaskSpec spec = new TaskSpec();
        spec.namespace = "indexer";
        spec.taskId = info.stream();
        spec.topic = info.topic();
        spec.partitions = info.partitions();
        spec.minPartitions = info.partitions();
        spec.maxPartitions = info.partitions();
        spec.targetRps = 0;                  // 索引器不参与自动扩缩(分区数随 topic 定)
        spec.handlerRef = new TaskSpec.HandlerRef();
        spec.handlerRef.type = Enums.HandlerType.SPI;
        spec.handlerRef.ref = "anchor-sampler";
        spec.handover = Enums.Handover.AGGRESSIVE;  // 锚点/水位 upsert、污染 INSERT IGNORE 均幂等,重放安全
        spec.bizMeta = new HashMap<>();
        spec.bizMeta.put("epoch", String.valueOf(executorEnvironment.epoch()));
        spec.bizMeta.put("groupId", executorEnvironment.groupId());
        spec.bizMeta.put("fromBeginning", String.valueOf(executorEnvironment.fromBeginning()));
        worker.submitSpec(spec);
        log.info("[scheduler] 提交 indexer TaskSpec: stream={} topic={} partitions={}",
                info.stream(), info.topic(), info.partitions());
    }

    /** 流注销 → 注销 spec(框架推进 DRAINING → 回收执行器)。 */
    public void cancelIndexTask(String stream) {
        worker.deleteSpec("indexer", stream);
        log.info("[scheduler] 注销 indexer TaskSpec: stream={}", stream);
    }

    /** 重建:登记流全量 → TaskSpec(重启/etcd 丢失后框架自愈)。 */
    public void resubmitAll() {
        for (StreamInfo info : registry.list()) {
            if (info.partitions() > 0) {
                submitIndexTask(info);
            }
        }
    }

    /**
     * 流扫描统一入口(StreamsController 调用):扫描一轮,随后把登记流提交为 TaskSpec。
     *
     * @return 本轮新登记 / 更新的流数量
     */
    public int scanAndSubmit() {
        int scanned = scanner.scanOnce();
        resubmitAll();
        return scanned;
    }
}

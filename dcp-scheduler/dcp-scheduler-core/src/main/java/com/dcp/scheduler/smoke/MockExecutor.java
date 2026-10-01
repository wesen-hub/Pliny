package com.dcp.scheduler.smoke;

import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.scheduler.model.ExecutorSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 冒烟用内存执行器:模拟消费,每 500ms 处理一批。
 * 计数即提交位点(模拟逐条提交)——位点自管,框架只拉四计数器。
 * 卡死模拟:STUCK 注册表内的分区 metrics() 抛异常 → 快照停更 = 活性丢失(执行器假死)。
 * 速率控制:RATE 注册表固定每批条数(扩缩容水位冒烟用);0 = 停产(模拟缩容切路由后流量停)。
 * 生产水位:PRODUCED 模拟 Kafka 生产端累积条数(供 MockLagSource 报 endOffset);
 *   消费语义 = 尽力消费积压(每批能力 batch,不超过积压),停产/注销后积压清空即 lag 归零。
 */
public class MockExecutor implements PartitionExecutor {

    private static final Logger log = LoggerFactory.getLogger(MockExecutor.class);

    /** 卡死分区注册表(key = taskId/partId),进程内;metrics() 命中即抛异常。 */
    public static final Set<String> STUCK = ConcurrentHashMap.newKeySet();

    /** 固定速率注册表(key = taskId/partId → 每批条数);0 = 停产。缺省随机。 */
    public static final Map<String, Integer> RATE = new ConcurrentHashMap<>();

    /** 任务级生产停止注册表(key = taskId):注销场景删 spec 后停产才能追平。 */
    public static final Set<String> STOPPED = ConcurrentHashMap.newKeySet();

    /** 生产水位(key = taskId/partId → 累积生产条数 = mock Kafka endOffset)。 */
    public static final Map<String, AtomicLong> PRODUCED = new ConcurrentHashMap<>();

    /** 错误率(每批错误概率,缺省 2%;smoke --err-rate 调高验证 ERROR_RATE 告警巡检)。 */
    public static volatile double ERR_RATE = 0.02;

    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong latency = new AtomicLong();
    private final Random rnd = new Random();

    private volatile ExecutorContext ctx;
    private volatile boolean stopped = true;
    private Thread worker;

    @Override
    public synchronized void start(ExecutorContext ctx) {
        this.ctx = ctx;
        stopped = false;
        worker = new Thread(() -> {
            while (!stopped) {
                try {
                    Thread.sleep(500);
                    Integer rate = RATE.get(ctx.taskId + "/" + ctx.partId);
                    long batch = rate != null && rate > 0 ? rate : 1 + rnd.nextInt(10); // 消费能力(停产后仍尽力清积压)
                    boolean producing = !STOPPED.contains(ctx.taskId) && (rate == null || rate > 0);
                    String k = ctx.taskId + "/" + ctx.partId;
                    AtomicLong prod = PRODUCED.computeIfAbsent(k, x -> new AtomicLong());
                    if (producing) prod.addAndGet(batch);
                    long backlog = prod.get() - processed.get();
                    long consume = Math.min(batch, backlog);
                    if (consume > 0) {
                        processed.addAndGet(consume);
                        latency.addAndGet(consume * (10 + rnd.nextInt(40)));
                        // 逐条按概率注入错误(错误率≈--err-rate,告警语义直观)
                        if (ERR_RATE > 0) {
                            for (long i = 0; i < consume; i++) {
                                if (rnd.nextInt(100) < ERR_RATE * 100) errors.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "mock-exec-" + ctx.taskId + "-" + ctx.partId);
        worker.setDaemon(true);
        worker.start();
        log.info("[smoke] 执行器启动 {}/{}/{}", ctx.ns, ctx.taskId, ctx.partId);
    }

    @Override
    public synchronized void stop() {
        stopped = true;
        if (worker != null) worker.interrupt();
        log.info("[smoke] 执行器停止 {}/{}/{}", ctx.ns, ctx.taskId, ctx.partId);
    }

    @Override
    public ExecutorSnapshot metrics() {
        if (STUCK.contains(ctx.taskId + "/" + ctx.partId)) {
            throw new IllegalStateException("mock stuck: 模拟执行器卡死(快照停更)");
        }
        // 提交位点 = 已处理数(模拟逐条提交)
        return ExecutorSnapshot.of(ctx.taskId, ctx.partId, processed.get(),
                processed.get(), errors.get(), latency.get(), stopped);
    }
}

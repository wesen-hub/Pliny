package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

/**
 * 消费-写库解耦层(有界队列 + 共享写线程池 + 按物理表攒批):
 *
 * <pre>
 *   消费者线程(每逻辑表 1 个, 见 SyncTopicRegistry)
 *      → 解析/路由 → BatchEntry(TableOp 列表 + 原始记录 + 分区 offset) → offer 有界队列
 *      → 队列满 → 调用方 pause 分区等待 → 有空位 resume
 *
 *   共享写线程池(write-threads 个, 不分逻辑表):
 *      轮询所有逻辑表队列 → 按物理表攒批(Accumulator)
 *      → 够批(batch-size)或够时(batch-timeout-ms) → flush 多值 INSERT(事务)
 *      → 更新 per-partition 已写 offset 游标
 *
 *   offset 提交(见 {@link #tryCommit(Consumer)}): 由**消费者线程**(容器监听线程)周期调用
 *   commitSync 提交游标 —— commitSync 要求调用者是合法组成员(同线程合法);
 *   历史上用"独立 committer consumer"提交, 但该 consumer 从不 poll/join 消费组,
 *   组内有活跃成员时提交必然 UNKNOWN_MEMBER_ID(缺陷, 已废弃)。
 *
 *   支持水平扩容: 实例无状态, 新实例启动注册队列即参与; 分区预置后只加实例即可。
 *   ack 语义: 不依赖 Spring Acknowledgment(其提交 poll position 会超前于写库进度),
 *   提交值 = 已写游标 + 1 → 崩溃时未写部分重放, 幂等吸收。
 * </pre>
 */
@Slf4j
@Service
public class BatchWriteService {

    /** 队列元素: 一"批"已解析记录(同一逻辑表) */
    public record BatchEntry(String logicalTable, List<AtomicUpsertDao.TableOp> ops,
                             List<ConsumerRecord<String, String>> rawRecords,
                             Map<TopicPartition, Long> offsets) {
    }

    /** 攒批桶: 同一物理表的待写操作 + 该批覆盖的分区 offset(取最大值) */
    private static class Accumulator {
        final String table;
        final String logicalTable;
        final List<AtomicUpsertDao.TableOp> ops = new ArrayList<>();
        final Map<TopicPartition, Long> offsets = new HashMap<>();
        final long firstTime;

        Accumulator(String table, String logicalTable) {
            this.table = table;
            this.logicalTable = logicalTable;
            this.firstTime = System.currentTimeMillis();
        }
    }

    private final TargetDbRegistry targetDb;
    private final DltService dltService;

    @Value("${dcp.sync.buffer.queue-capacity:2000}")
    private int queueCapacity;
    @Value("${dcp.sync.buffer.batch-size:500}")
    private int batchSize;
    @Value("${dcp.sync.buffer.batch-timeout-ms:100}")
    private long batchTimeoutMs;
    @Value("${dcp.sync.buffer.write-threads:4}")
    private int writeThreads;
    @Value("${dcp.sync.buffer.commit-interval-ms:5000}")
    private long commitIntervalMs;
    /** 提交前等待写线程追平本次 poll 的最大等待(批小秒级内必追平; 超时先提交已写部分) */
    @Value("${dcp.sync.buffer.flush-wait-ms:15000}")
    private long flushWaitMs;
    @Value("${dcp.sync.buffer.drain-per-queue:200}")
    private int drainPerQueue;

    /** 每逻辑表一个有界队列 */
    private final Map<String, ArrayBlockingQueue<BatchEntry>> queues = new ConcurrentHashMap<>();
    /** per-partition 已写 offset 游标(写库成功后更新; 消费者线程周期提交) */
    private final Map<TopicPartition, Long> offsetCursor = new ConcurrentHashMap<>();
    /** 组 → 最近一次成功提交时间(commitSync 节流, 防每批都提交) */
    private final Map<String, Long> lastCommitAt = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private ExecutorService writerPool;

    public BatchWriteService(TargetDbRegistry targetDb,
                             DltService dltService) {
        this.targetDb = targetDb;
        this.dltService = dltService;
    }

    @PostConstruct
    public void init() {
        writerPool = Executors.newFixedThreadPool(Math.max(1, writeThreads), r -> {
            Thread t = new Thread(r, "dcp-batch-writer");
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < writeThreads; i++) {
            writerPool.submit(this::writeLoop);
        }
        log.info("BatchWriteService 启动: writeThreads={} batchSize={} batchTimeout={}ms queueCapacity={} commitInterval={}ms",
                writeThreads, batchSize, batchTimeoutMs, queueCapacity, commitIntervalMs);
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        if (writerPool != null) {
            writerPool.shutdown();
            try {
                writerPool.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** 每逻辑表一个队列(消费者线程注册时调用); 返回队列 */
    public ArrayBlockingQueue<BatchEntry> ensureQueue(String logicalTable) {
        return queues.computeIfAbsent(logicalTable, k -> new ArrayBlockingQueue<>(queueCapacity));
    }

    /** 消费者线程投递; 满则返回 false → 调用方 pause 分区等待后重试 */
    public boolean offer(String logicalTable, BatchEntry entry) {
        ArrayBlockingQueue<BatchEntry> q = queues.get(logicalTable);
        return q != null && q.offer(entry);
    }

    /** 队列剩余容量(背压/监控) */
    public int remaining(String logicalTable) {
        ArrayBlockingQueue<BatchEntry> q = queues.get(logicalTable);
        return q == null ? 0 : q.remainingCapacity();
    }

    /** 写线程主循环: 轮询所有队列 → 按物理表攒批 → 够批/够时 flush → 周期提交 offset */
    private void writeLoop() {
        Map<String, Accumulator> buckets = new HashMap<>();

        while (running) {
            try {
                // ① 轮询所有逻辑表队列(drain 限流, 避免单队列独占写线程)
                for (ArrayBlockingQueue<BatchEntry> q : queues.values()) {
                    List<BatchEntry> drained = new ArrayList<>(drainPerQueue);
                    q.drainTo(drained, drainPerQueue);
                    for (BatchEntry entry : drained) {
                        for (AtomicUpsertDao.TableOp op : entry.ops()) {
                            Accumulator acc = buckets.computeIfAbsent(op.table(),
                                    t -> new Accumulator(t, entry.logicalTable()));
                            acc.ops.add(op);
                            mergeOffsets(acc.offsets, entry.offsets());
                        }
                    }
                }
                // ② flush 判定: 够批 或 够时
                long now = System.currentTimeMillis();
                buckets.entrySet().removeIf(e -> {
                    Accumulator acc = e.getValue();
                    if (acc.ops.size() >= batchSize || now - acc.firstTime >= batchTimeoutMs) {
                        flush(acc);
                        return true;
                    }
                    return false;
                });
                // ③ 无桶待刷则短暂休眠(提交不在此线程: 由消费者线程 commitAfterOffer 驱动, 见 SyncTopicRegistry)
                if (buckets.isEmpty()) {
                    Thread.sleep(5);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                log.error("写线程循环异常: {}", ex.getMessage(), ex);
            }
        }
        // 退出前 flush 剩余
        for (Accumulator acc : buckets.values()) {
            try {
                flush(acc);
            } catch (Exception ignored) {
            }
        }
        log.info("写线程退出");
    }

    /** flush 一个物理表的攒批: 多值 INSERT(事务) → 已写游标 */
    private void flush(Accumulator acc) {
        if (acc.ops.isEmpty()) {
            return;
        }
        try {
            // 多目标库: 按逻辑表路由到对应库的 DAO(攒批桶内同表 = 同逻辑表 = 同库)
            targetDb.daoOf(acc.logicalTable).applyMultiValue(acc.ops);
            // 写库成功 → 更新已写 offset 游标(提交由消费者线程执行)
            acc.offsets.forEach((tp, off) -> offsetCursor.merge(tp, off, Math::max));
            log.debug("flush {}: {} 条", acc.table, acc.ops.size());
        } catch (Exception e) {
            // 批量失败 → 死信兜底(不阻塞流水线); 游标不更新 → 下轮提交仍从旧位点(重放由幂等吸收)
            log.error("flush 失败 {} ({}条): {}, 写死信", acc.table, acc.ops.size(), e.getMessage());
            try {
                dltService.saveRaw(acc.logicalTable, e.getMessage(), acc.ops.size());
            } catch (Exception ex) {
                log.error("死信落库失败: {}", ex.getMessage());
            }
        }
    }

    private void mergeOffsets(Map<TopicPartition, Long> target, Map<TopicPartition, Long> src) {
        if (src == null) {
            return;
        }
        src.forEach((tp, off) -> target.merge(tp, off, Math::max));
    }

    /**
     * 消费者线程回调(容器监听线程, 见 SyncTopicRegistry): 等待本次 poll 的记录被写线程追平后,
     * 把"已写 offset 游标"中属于本 consumer assignment 的分区提交(下一条消费位置 = 已写最大 offset + 1)。
     *
     * <p>必须在持有该 KafkaConsumer 的线程内调用 —— commitSync 需要合法的组成员身份;
     * 历史"独立 committer consumer"从不 poll/join 组, 组内有活跃成员时提交必失败
     * (UNKNOWN_MEMBER_ID), 该做法已废弃。
     *
     * @param offered 本次 poll 投递的记录, 按分区取最大 offset(等待写线程追平到它才提交;
     *                超过 {@code flushWaitMs} 则尽力提交已写部分, 下轮/下批再追)。
     */
    public void commitAfterOffer(Consumer<?, ?> consumer,
                                 Map<TopicPartition, Long> offered) {
        if (consumer == null || offered == null || offered.isEmpty() || offsetCursor.isEmpty()) {
            return;
        }
        long deadline = System.currentTimeMillis() + flushWaitMs;
        boolean caughtUp = true;
        while (System.currentTimeMillis() < deadline) {
            caughtUp = true;
            for (Map.Entry<TopicPartition, Long> e : offered.entrySet()) {
                Long written = offsetCursor.get(e.getKey());
                if (written == null || written < e.getValue()) {
                    caughtUp = false;
                    break;
                }
            }
            if (caughtUp) {
                break;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (!caughtUp) {
            // 追平超时: 只发生在大批重放/补列等瞬时慢 flush; 未追平尾部由空闲事件(commitIdle)补交,
            // 不再依赖下一条消息/重启触发。
            log.warn("写追平超时({}ms), 本次仅提交已追平分区: 未追平尾部将由空闲补交", flushWaitMs);
        }
        // 本方法已是唯一提交入口且按 poll 触发 → 一律直接提交(带节流分支会在追平超时后
        // 被 5s 节流跳过, 若无下一条消息则尾部 offset 永不提交, lag 恒卡 1 以上)
        doCommit(consumer);
    }

    /** 空闲补交(消费者线程, 容器空闲事件回调见 SyncTopicRegistry#onIdle):
     *  写线程已把"超时未追平"的尾部刷完(游标已更新) → 这里补提交;
     *  专门解决"尾批超时后再无新消息 → 提交永不再触发"的假卡 lag, 无需重启/新消息。
     *  轻节流: 1s 内刚成功提交过则跳过(空闲事件约 1s 一次)。 */
    public void commitIdle(Consumer<?, ?> consumer) {
        if (consumer == null || offsetCursor.isEmpty()) {
            return;
        }
        String group = consumer.groupMetadata().groupId();
        Long last = lastCommitAt.get(group);
        if (last != null && System.currentTimeMillis() - last < 1000) {
            return;
        }
        doCommit(consumer);
    }

    /** 提交游标中属于本 consumer assignment 的分区(提交值 = 已写游标 + 1, 绝不超前) */
    private void doCommit(Consumer<?, ?> consumer) {
        String group = consumer.groupMetadata().groupId();
        long now = System.currentTimeMillis();
        java.util.Set<TopicPartition> owned = consumer.assignment();
        if (owned.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        offsetCursor.forEach((tp, off) -> {
            if (owned.contains(tp)) {
                toCommit.put(tp, new OffsetAndMetadata(off + 1));
            }
        });
        if (toCommit.isEmpty()) {
            return;
        }
        try {
            consumer.commitSync(toCommit);
            lastCommitAt.put(group, now);
            log.debug("offset 已提交 group={} {} 个分区", group, toCommit.size());
        } catch (Exception ex) {
            // 重平衡窗口期的正常噪音 → 下轮重试(未提交部分崩溃重放由幂等吸收)
            log.warn("offset 提交失败 group={}(下轮重试): {}", group, ex.getMessage());
        }
    }
}

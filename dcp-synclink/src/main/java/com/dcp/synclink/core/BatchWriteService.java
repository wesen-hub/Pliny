package com.dcp.synclink.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.*;

/**
 * 消费-写库解耦层(有界队列 + 共享写线程池 + 按物理表攒批):
 *
 * <pre>
 *   消费者线程(框架每分区一线程, 见 schedulerbridge.SyncWriterExecutor)
 *      → 解析/路由 → BatchEntry(TableOp 列表 + per-op 原始记录 OpRef) → offer 有界队列
 *      → 队列满 → 调用方 pause 分区等待 → 有空位 resume
 *
 *   共享写线程池(write-threads 个, 不分逻辑表):
 *      轮询所有逻辑表队列 → 按物理表攒批(Accumulator)
 *      → 够批(batch-size)或够时(batch-timeout-ms) → flush 多值 INSERT(事务)
 *      → 更新 per-partition 已写 offset 游标
 *
 *   写失败处置(新方案 G2): 攒批失败 → 逐条重试隔离, 重试仍失败的逐条入死信(记录失败原因),
 *   不阻塞后续数据流(全部条处理完毕(写成功/入死信)即推进游标)。
 *
 *   offset 提交(见 {@link #tryCommit(Consumer)}): 由**消费者线程**(框架执行器线程)周期调用
 *   commitSync 提交游标 —— commitSync 要求调用者是合法组成员(同线程合法);
 *   历史上用"独立 committer consumer"提交, 但该 consumer 从不 poll/join 消费组,
 *   组内有活跃成员时提交必然 UNKNOWN_MEMBER_ID(缺陷, 已废弃)。
 *
 *   支持水平扩容: 实例无状态, 新实例启动注册队列即参与; 分区预置后只加实例即可。
 *   ack 语义: 不依赖 Spring Acknowledgment(其提交 poll position 会超前于写库进度),
 *   提交值 = 已写游标 + 1 → 崩溃时未写部分重放, 幂等吸收。
 *
 *   无 Spring 装配: 配置参数与 init/shutdown 生命周期由 SynclinkComponents 显式管理。
 * </pre>
 */
@Slf4j
public class BatchWriteService {

    /** 队列元素: 一"批"已解析记录(同一逻辑表, ops 与 opRefs 一一对应);
     *  snapshot=true 走批路径三步式模板, false 走流路径多值 upsert */
    public record BatchEntry(String logicalTable, List<AtomicUpsertDao.TableOp> ops,
                             List<OpRef> opRefs, boolean snapshot) {
    }

    /** 单 op 的原始记录引用(per-op 一一对应: 死信留证取 raw, 游标推进取 raw 的 topic/partition/offset) */
    public record OpRef(ConsumerRecord<String, String> raw) {
    }

    /** 攒批桶键: 同一物理表 + 同一批流模板(快照批与增量批分开攒, 各自 flush) */
    private record BucketKey(String table, boolean snapshot) {
    }

    /** 攒批桶: 同一物理表的待写操作(与原始记录一一对应, 供失败逐条隔离入死信) + 该批覆盖的分区 offset(取最大值) */
    private static class Accumulator {
        final String table;
        final String logicalTable;
        final boolean snapshot;
        final List<AtomicUpsertDao.TableOp> ops = new ArrayList<>();
        final List<ConsumerRecord<String, String>> records = new ArrayList<>();
        final Map<TopicPartition, Long> offsets = new HashMap<>();
        final long firstTime;

        Accumulator(String table, String logicalTable, boolean snapshot) {
            this.table = table;
            this.logicalTable = logicalTable;
            this.snapshot = snapshot;
            this.firstTime = System.currentTimeMillis();
        }
    }

    private final TargetDbRegistry targetDb;
    private final DltService dltService;

    private final int queueCapacity;
    private final int batchSize;
    private final long batchTimeoutMs;
    private final int writeThreads;
    private final long commitIntervalMs;
    private final long flushWaitMs;
    private final int drainPerQueue;

    /** 每逻辑表一个有界队列 */
    private final Map<String, ArrayBlockingQueue<BatchEntry>> queues = new ConcurrentHashMap<>();
    /** per-partition 已写 offset 游标(写库成功后更新; 消费者线程周期提交) */
    private final Map<TopicPartition, Long> offsetCursor = new ConcurrentHashMap<>();
    /** 组 → 最近一次成功提交时间(commitSync 节流, 防每批都提交) */
    private final Map<String, Long> lastCommitAt = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private ExecutorService writerPool;

    public BatchWriteService(TargetDbRegistry targetDb,
                             DltService dltService,
                             int queueCapacity, int batchSize, long batchTimeoutMs,
                             int writeThreads, long commitIntervalMs, long flushWaitMs,
                             int drainPerQueue) {
        this.targetDb = targetDb;
        this.dltService = dltService;
        this.queueCapacity = queueCapacity;
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.writeThreads = writeThreads;
        this.commitIntervalMs = commitIntervalMs;
        this.flushWaitMs = flushWaitMs;
        this.drainPerQueue = drainPerQueue;
    }

    /** 启动写线程池(原 @PostConstruct, 由 SynclinkComponents 显式调用) */
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

    /** 关闭写线程池(原 @PreDestroy, 由 SynclinkComponents 显式调用) */
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

    /** 已写 offset 游标查询(调度框架执行器 metrics 上报 committedOffset;无记录返回 -1)。 */
    public long writtenOffset(String topic, int partition) {
        Long off = offsetCursor.get(new TopicPartition(topic, partition));
        return off == null ? -1 : off;
    }

    /** 无需写库的记录(如 TiCDC 水位事件/已落死信留证的消息)跳过时推进游标:
     *  直接视为已处理参与提交, 避免"尾批之后无数据 → 游标永不追平"的假卡 lag。 */
    public void ackOffsets(Map<TopicPartition, Long> offsets) {
        if (offsets != null) {
            offsets.forEach((tp, off) -> offsetCursor.merge(tp, off, Math::max));
        }
    }

    /** 写线程主循环: 轮询所有队列 → 按物理表攒批 → 够批/够时 flush → 周期提交 offset */
    private void writeLoop() {
        Map<BucketKey, Accumulator> buckets = new HashMap<>();

        while (running) {
            try {
                // ① 轮询所有逻辑表队列(drain 限流, 避免单队列独占写线程)
                for (ArrayBlockingQueue<BatchEntry> q : queues.values()) {
                    List<BatchEntry> drained = new ArrayList<>(drainPerQueue);
                    q.drainTo(drained, drainPerQueue);
                    for (BatchEntry entry : drained) {
                        for (int i = 0; i < entry.ops().size(); i++) {
                            AtomicUpsertDao.TableOp op = entry.ops().get(i);
                            BucketKey key = new BucketKey(op.table(), entry.snapshot());
                            Accumulator acc = buckets.computeIfAbsent(key,
                                    k -> new Accumulator(k.table(), entry.logicalTable(), k.snapshot()));
                            acc.ops.add(op);
                            BatchWriteService.OpRef ref = i < entry.opRefs().size() ? entry.opRefs().get(i) : null;
                            // per-op 原始记录(失败逐条隔离入死信留证)
                            if (ref != null && ref.raw() != null) {
                                acc.records.add(ref.raw());
                            }
                            // per-op offset(缺陷 4): 桶只合并本桶 op 的 offset,
                            // 消灭整批 max offset 塞进每个物理表桶导致的跨表提前推进
                            if (ref != null && ref.raw() != null) {
                                acc.offsets.merge(new TopicPartition(ref.raw().topic(), ref.raw().partition()),
                                        ref.raw().offset(), Math::max);
                            }
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
                // ③ 无桶待刷则短暂休眠(提交不在此线程: 由消费者线程 commitAfterOffer 驱动, 见 SyncWriterExecutor)
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

    /** flush 一个物理表的攒批: 多值 INSERT(事务) → 已写游标; 失败 → 逐条隔离(重试/入死信) */
    private void flush(Accumulator acc) {
        if (acc.ops.isEmpty()) {
            return;
        }
        try {
            // 多目标库: 按逻辑表路由到对应库的 DAO(攒批桶内同表 = 同逻辑表 = 同库);
            // 快照批走批路径三步式(探存在 + 条件 UPDATE + 双条件 INSERT), 增量批走流路径多值 upsert
            if (acc.snapshot) {
                targetDb.daoOf(acc.logicalTable).applySnapshotBatch(acc.ops);
            } else {
                targetDb.daoOf(acc.logicalTable).applyMultiValue(acc.ops);
            }
            // 写库成功 → 更新已写 offset 游标(提交由消费者线程执行)
            acc.offsets.forEach((tp, off) -> offsetCursor.merge(tp, off, Math::max));
            log.debug("flush {}: {} 条", acc.table, acc.ops.size());
        } catch (Exception e) {
            // 攒批失败(G2 新方案): 逐条重试隔离, 重试仍失败的逐条入死信(记录失败原因),
            // 不阻塞后续数据流; 全部条处理完毕(写成功/入死信)即推进游标
            log.error("flush 失败 {} ({}条), 转逐条隔离: {}", acc.table, acc.ops.size(), e.getMessage());
            isolate(acc, e.getMessage());
        }
    }
    
    /**
     * 逐条隔离: 每条单事务重试; 仍失败 → 落死信(带原始 record 与失败原因)。
     * 处理完(写成功或入死信)后推进游标 —— 失败不再阻塞后续数据流, 由人工从死信处理。
     */
    private void isolate(Accumulator acc, String batchError) {
        int ok = 0;
        int dlt = 0;
        for (int i = 0; i < acc.ops.size(); i++) {
            AtomicUpsertDao.TableOp op = acc.ops.get(i);
            try {
                targetDb.daoOf(acc.logicalTable).apply(op);
                ok++;
            } catch (Exception one) {
                dlt++;
                String reason = "攒批失败(" + batchError + ")逐条重试仍失败: " + one.getMessage();
                try {
                    if (i < acc.records.size() && acc.records.get(i) != null) {
                        dltService.save(acc.records.get(i), reason);
                    } else {
                        dltService.saveRaw(acc.logicalTable, reason, 1);
                    }
                } catch (Exception dltEx) {
                    log.error("死信落库失败: {}", dltEx.getMessage());
                }
            }
        }
        if (dlt > 0) {
            log.warn("逐条隔离 {}: 成功 {} 条, 死信 {} 条(批量错误: {})", acc.table, ok, dlt, batchError);
        }
        acc.offsets.forEach((tp, off) -> offsetCursor.merge(tp, off, Math::max));
    }

    /**
     * 消费者线程回调(框架执行器线程, 见 SyncWriterExecutor): 等待本次 poll 的记录被写线程追平后,
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

    /** 空闲补交(消费者线程, 框架执行器线程空闲时调用, 见 SyncWriterExecutor):
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

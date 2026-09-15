package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 单元运行器(v2 计算面核心)。
 *
 * <p><b>单元</b> = 1 个消费源 + 1 个线程 + 1 个 logic 实例 + 1 份租约(D9)。本类负责:
 * <pre>
 *   poll → 解析 → 水位更新 → 契约裁剪 → process() → commitSync
 *         ↘ 异常:不提交 → 退避重试 N 次 → 死信 + 告警(D11)
 *   每轮:续租(失败即自停)、心跳(平台据此判 RUNNING / 停滞)
 * </pre>
 *
 * <p><b>提交语义</b>:{@code process()} 正常返回才提交;提交的是"下一条要读的位置"。
 * 空批也会提交(仅当位置前进时),避免重启后大量重复。
 */
public class UnitRunner {

    /** 单元状态机。 */
    public enum State { INIT, RUNNING, STOPPED, FAILED, LEASE_LOST, STOPPING }

    private static final class Unit {
        final java.util.concurrent.atomic.AtomicBoolean started = new java.util.concurrent.atomic.AtomicBoolean();
        volatile State state = State.INIT;
        Thread thread;
        RecordSource source;
        UnitRuntime rt;
        final CountDownLatch done = new CountDownLatch(1);
        final Map<Integer, Long> position = new ConcurrentHashMap<>();
        volatile long lastCommitOffset = -1L;
        volatile long lastTso = 0L;
        volatile String lastError;
    }

    private final LogicLoader loader;
    private final MessageDecoder decoder;
    private final ContractFilter filter;
    private final RecordSourceFactory sourceFactory;
    private final LeaseGuard leaseGuard;
    private final Heartbeat heartbeat;
    private final WatermarkProvider watermarks;
    private final DeadLetterSink deadLetterSink;
    private final UnitMetrics metrics;
    private final Function<String, SdkLogger> logFactory;
    /** 每单元一个契约视图提供者(生产:从控制面拉;缺省:无限制视图 —— **生产不得缺省**)。 */
    private final ContractProviderFactory contractProviders;
    private final Map<String, Unit> units = new ConcurrentHashMap<>();

    /** 兼容构造器:契约提供者缺省为无限制视图(仅骨架/自检用)。 */
    public UnitRunner(LogicLoader loader, MessageDecoder decoder, ContractFilter filter,
                      RecordSourceFactory sourceFactory, LeaseGuard leaseGuard, Heartbeat heartbeat,
                      WatermarkProvider watermarks, DeadLetterSink deadLetterSink,
                      UnitMetrics metrics, Function<String, SdkLogger> logFactory) {
        this(loader, decoder, filter, sourceFactory, leaseGuard, heartbeat, watermarks, deadLetterSink,
                metrics, logFactory, ContractProviderFactory.permissive());
    }

    public UnitRunner(LogicLoader loader, MessageDecoder decoder, ContractFilter filter,
                      RecordSourceFactory sourceFactory, LeaseGuard leaseGuard, Heartbeat heartbeat,
                      WatermarkProvider watermarks, DeadLetterSink deadLetterSink,
                      UnitMetrics metrics, Function<String, SdkLogger> logFactory,
                      ContractProviderFactory contractProviders) {
        this.loader = loader; this.decoder = decoder; this.filter = filter;
        this.sourceFactory = sourceFactory; this.leaseGuard = leaseGuard; this.heartbeat = heartbeat;
        this.watermarks = watermarks; this.deadLetterSink = deadLetterSink; this.metrics = metrics;
        this.logFactory = logFactory;
        this.contractProviders = contractProviders == null
                ? ContractProviderFactory.permissive() : contractProviders;
    }

    // ================= 生命周期 =================

    /** 创建并启动一个单元(对应控制面的 CreateUnit)。 */
    public void start(CreateUnitCmd cmd) {
        Unit u = units.computeIfAbsent(cmd.unitId(), k -> new Unit());
        if (!u.started.compareAndSet(false, true)) return;  // 幂等:同 unitId 重复下发无副作用(含并发下发)
        u.state = State.INIT;
        u.thread = new Thread(() -> runLoop(cmd, u), "unit-" + cmd.unitId());
        u.thread.setDaemon(true);
        u.thread.start();
    }

    /**
     * 优雅停机:停止消费 → 提交 → close()。
     *
     * <p>幂等且**不覆盖已终结状态**(STOPPED/FAILED/LEASE_LOST)—— 否则已退出的单元会被改写成 STOPPING。
     */
    public void stop(String unitId) {
        Unit u = units.get(unitId);
        if (u == null) return;
        State cur = u.state;
        if (cur == State.STOPPED || cur == State.FAILED || cur == State.LEASE_LOST) return;
        u.state = State.STOPPING;
        if (u.rt != null) u.rt.stop();
        if (u.source != null) u.source.wakeup();
    }

    /** 等待单元退出。 */
    public boolean awaitTermination(String unitId, long millis) throws InterruptedException {
        Unit u = units.get(unitId);
        return u != null && u.done.await(millis, TimeUnit.MILLISECONDS);
    }

    public State state(String unitId) {
        Unit u = units.get(unitId);
        return u == null ? null : u.state;
    }

    public long lastCommittedOffset(String unitId) {
        Unit u = units.get(unitId);
        return u == null ? -1L : u.lastCommitOffset;
    }

    public long lastTso(String unitId) {
        Unit u = units.get(unitId);
        return u == null ? 0L : u.lastTso;
    }

    public String lastError(String unitId) {
        Unit u = units.get(unitId);
        return u == null ? null : u.lastError;
    }

    // ================= 主循环 =================

    private void runLoop(CreateUnitCmd cmd, Unit u) {
        SdkLogger log = logFactory.apply(cmd.unitId());
        LoadedLogic logicRef = null;
        try {
            LoadedLogic loaded = loader.load(cmd.logic());         // ① 加载(可来自 classpath / SPI / jar)
            logicRef = loaded;
            ConsumerLogic logic = loaded.logic();                  //    一单元一实例
            UnitRuntime rt = new UnitRuntime(cmd.unitId(), cmd.taskId(), cmd.topic(), cmd.partitions(),
                    cmd.consumerGroup(), cmd.logic().logicId(), cmd.logic().version(), cmd.params(),
                    contractProviders.forUnit(cmd), watermarks, deadLetterSink, metrics, log);
            u.rt = rt;
            if (u.state == State.STOPPING) rt.stop();              // 启动前就收到 stop → 不进入消费
            rt.contractProvider().current();                       // ①' 契约必须可获取:拉不到且无缓存 → 直接失败
                                                                   //     (fail-closed:宁可不启动,也不能无契约消费)
            logic.init(new InitContextImpl(rt));                   // ② 初始化(建连接池 / 加载规则)

            RecordSource source = sourceFactory.create(cmd);       // ③ 消费源 + 注入起点
            u.source = source;
            Map<Integer, Long> positions = new LinkedHashMap<>();
            for (int p : cmd.partitions()) {
                long start = cmd.startOffsets().getOrDefault(p, 0L);
                source.seek(p, start);
                positions.put(p, start);
                u.position.put(p, start);
            }
            u.state = State.RUNNING;
            log.info("unit started task={} topic={} partitions={} logic={}",
                    cmd.taskId(), cmd.topic(), cmd.partitions(), cmd.logic());

            while (rt.running()) {
                if (!leaseGuard.renew(cmd.unitId())) {                         // ④ 续租失败 → 自停(防双消费)
                    u.state = State.LEASE_LOST;
                    u.lastError = "lease lost";
                    log.warn("lease lost, unit stops consuming");
                    break;
                }

                List<RawMessage> raw = source.poll(cmd.pollMillis());
                if (raw.isEmpty()) {                               // ⑤ 空批:位置没动就不提交
                    commitIfAdvanced(rt, u, source, log);
                    heartbeat.send(cmd.unitId(), firstPartition(cmd), u.lastCommitOffset, u.lastTso);
                    continue;
                }

                List<DecodedMessage> events = new ArrayList<>(raw.size());
                for (RawMessage m : raw) {
                    DecodedMessage d = decoder.decode(m);
                    if (d.kind() == DecodedMessage.Kind.WATERMARK) {
                        watermarks.update(d.partition(), d.tso()); // ⑥ 水位不进入业务逻辑
                    } else if (d.kind() == DecodedMessage.Kind.EVENT) {
                        events.add(d);
                    }
                    u.lastTso = Math.max(u.lastTso, d.tso());
                    u.position.put(m.partition(), m.offset() + 1);
                }

                List<ChangeRecord> records = filter.apply(                    // ⑦ 契约裁剪(列/行/密级)
                        rt.contractProvider().current(), cmd.stream(), events);

                if (records.isEmpty()) {
                    commitIfAdvanced(rt, u, source, log);
                    heartbeat.send(cmd.unitId(), firstPartition(cmd), u.lastCommitOffset, u.lastTso);
                    continue;
                }

                Batch batch = new BatchImpl(Mode.INCREMENTAL, cmd.stream(), records);
                boolean ok = processWithRetry(cmd, u, rt, logic, batch, log); // ⑧ 业务逻辑 + 重试

                if (ok || cmd.failure().onFailure() == FailurePolicy.OnFailure.SKIP) {
                    commitIfAdvanced(rt, u, source, log);                     // ⑨ 返回即提交
                } else {
                    u.state = State.FAILED;                                   //    不提交 → 停止(至少一次)
                    rt.stop();
                    break;
                }

                heartbeat.send(cmd.unitId(), firstPartition(cmd), u.lastCommitOffset, u.lastTso);
            }
        } catch (Throwable t) {
            u.state = State.FAILED;
            u.lastError = String.valueOf(t.getMessage());
            log.error("unit failed: " + cmd.unitId(), t);
        } finally {
            // 停机顺序:停 poll → 释放逻辑资源(close)→ 关消费源 → 落状态
            try {
                if (u.rt != null && u.rt.running()) u.rt.stop();
                if (logicRef != null && logicRef.logic() != null) logicRef.logic().close();
            } catch (Throwable t) {
                u.lastError = String.valueOf(t.getMessage());
                log.error("logic.close() failed", t);
            }
            try {
                if (u.source != null) u.source.close();
                if (u.state == State.RUNNING || u.state == State.STOPPING) u.state = State.STOPPED;
            } catch (Throwable ignored) {
                // 停机路径不掩盖真实错误
            }
            u.done.countDown();
            log.info("unit exited state={} lastCommitted={}", u.state, u.lastCommitOffset);
        }
    }

    /** 有界重试:失败不提交;重试耗尽 → 死信 + 告警。 */
    private boolean processWithRetry(CreateUnitCmd cmd, Unit u, UnitRuntime rt,
                                     ConsumerLogic logic, Batch batch, SdkLogger log) {
        FailurePolicy fp = cmd.failure();
        int attempts = Math.max(1, fp.maxRetries() + 1);
        long started = System.currentTimeMillis();
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                Runnable explicitCommit = () -> commitIfAdvanced(rt, u, u.source, log);
                UnitContextImpl ctx = new UnitContextImpl(rt, Mode.INCREMENTAL,
                        batch.records().isEmpty() ? firstPartition(cmd) : batch.records().get(0).partition(),
                        explicitCommit);
                logic.process(batch, ctx);
                metrics.batchProcessed(firstPartition(cmd), batch.size(),
                        System.currentTimeMillis() - started, u.lastTso);
                return true;
            } catch (Throwable t) {
                u.lastError = String.valueOf(t.getMessage());
                metrics.failure(firstPartition(cmd), t);
                log.warn("process failed attempt {}/{} : {}", attempt, attempts, t.toString());
                if (attempt < attempts) {
                    metrics.retry(firstPartition(cmd), attempt);
                    sleepQuietly(fp.backoffMillis());
                } else {
                    deadLetterSink.record(cmd.taskId(), cmd.unitId(), firstPartition(cmd),
                            u.lastCommitOffset, u.lastTso, null, t);       // 落死信,绝不静默丢弃
                    metrics.deadLetter(firstPartition(cmd));
                    log.error("dead letter recorded after " + attempts + " attempts", t);
                }
            }
        }
        return false;
    }

    private void commitIfAdvanced(UnitRuntime rt, Unit u, RecordSource source, SdkLogger log) {
        if (u.position.isEmpty()) return;
        long min = Long.MAX_VALUE;
        for (Map.Entry<Integer, Long> e : u.position.entrySet()) {
            min = Math.min(min, e.getValue());
        }
        if (min <= u.lastCommitOffset) return;                 // 位置未前进 → 不产生无意义提交
        for (Map.Entry<Integer, Long> e : u.position.entrySet()) {
            source.commitSync(e.getKey(), e.getValue());
        }
        u.lastCommitOffset = min;
        metrics.commit(firstPartition2(u), min);
        log.debug("committed offset={}", min);
    }

    private int firstPartition(CreateUnitCmd cmd) {
        return cmd.partitions().isEmpty() ? 0 : cmd.partitions().get(0);
    }

    private int firstPartition2(Unit u) {
        return u.position.isEmpty() ? 0 : u.position.keySet().iterator().next();
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) return;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}

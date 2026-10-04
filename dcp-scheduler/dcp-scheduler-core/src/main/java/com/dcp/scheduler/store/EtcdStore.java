package com.dcp.scheduler.store;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchResponse;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.common.exception.CompactedException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * etcd 封装:注册/lease 保活/读写/watch/CAS/竞选。
 * 唯一事实源;所有写路径基于事务或条件写。
 */
public class EtcdStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EtcdStore.class);

    private final Client client;

    public EtcdStore(String endpoints) {
        // jetcd 0.8.x 的 endpoints(String...) 按单个 URI 解析, 逗号分隔多地址需自行拆分
        this.client = Client.builder().endpoints(endpoints.split(",")).build();
    }

    public static EtcdStore connect(String endpoints) {
        return new EtcdStore(endpoints);
    }

    @Override
    public void close() {
        client.close();
    }

    // ══════════ lease ══════════

    /** 授予租约并启动自动续租。TTL 秒(注册 30s / 快照 30s / leader 30s)。 */
    public LeaseHandle grantLease(long ttlSec) {
        try {
            Lease lease = client.getLeaseClient(); // 注意:Lease 是共享客户端实例,不能 close
            io.etcd.jetcd.lease.LeaseGrantResponse resp = lease.grant(ttlSec).get(5, TimeUnit.SECONDS);
            long id = resp.getID();
            io.etcd.jetcd.support.CloseableClient keepAlive = lease.keepAlive(id, new StreamObserver<>() {
                @Override public void onNext(LeaseKeepAliveResponse r) { /* 续租成功 */ }
                @Override public void onError(Throwable t) {
                    log.warn("lease {} 续租失败: {}", id, t.getMessage());
                }
                @Override public void onCompleted() { /* 正常关闭 */ }
            });
            return new LeaseHandle(id, lease, keepAlive);
        } catch (Exception e) {
            throw new IllegalStateException("lease 授予失败", e);
        }
    }

    /** 租约句柄:close 仅停续租 + revoke 该租约,不影响共享 Lease 客户端上的其他租约。 */
    public static final class LeaseHandle implements AutoCloseable {
        public final long id;
        private final Lease lease;
        private final io.etcd.jetcd.support.CloseableClient keepAlive;
        LeaseHandle(long id, Lease lease, io.etcd.jetcd.support.CloseableClient keepAlive) {
            this.id = id; this.lease = lease; this.keepAlive = keepAlive;
        }
        @Override public void close() {
            try { keepAlive.close(); } catch (Exception ignored) {}
            try { lease.revoke(id); } catch (Exception ignored) { /* 异步 revoke,键随租约删除 */ }
        }
    }

    // ══════════ 基本读写 ══════════

    public void put(ByteSequence key, Object value) {
        try {
            client.getKVClient().put(key, ByteSequence.from(Json.toBytes(value))).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("put 失败: " + key, e);
        }
    }

    public void putLease(ByteSequence key, Object value, long leaseId) {
        try {
            client.getKVClient().put(key, ByteSequence.from(Json.toBytes(value)),
                    PutOption.builder().withLeaseId(leaseId).build()).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("putLease 失败: " + key, e);
        }
    }

    public void delete(ByteSequence key) {
        try {
            client.getKVClient().delete(key).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("delete 失败: " + key, e);
        }
    }

    /** 前缀清空(ns 删除等管理操作)。 */
    public void deletePrefix(ByteSequence prefix) {
        try {
            client.getKVClient().delete(prefix, DeleteOption.builder().withPrefix(prefix).build())
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("前缀清空失败: " + prefix, e);
        }
    }

    public Optional<byte[]> getBytes(ByteSequence key) {
        try {
            GetResponse r = client.getKVClient().get(key).get(5, TimeUnit.SECONDS);
            if (r.getKvs().isEmpty()) return Optional.empty();
            return Optional.of(r.getKvs().get(0).getValue().getBytes());
        } catch (Exception e) {
            throw new IllegalStateException("get 失败: " + key, e);
        }
    }

    public <T> Optional<T> getObj(ByteSequence key, Class<T> type) {
        return getBytes(key).map(b -> Json.fromBytes(b, type));
    }

    /** 前缀读:key(字符串) → value 字节。 */
    public Map<String, byte[]> getPrefix(ByteSequence prefix) {
        try {
            GetResponse r = client.getKVClient().get(prefix,
                    GetOption.builder().withPrefix(prefix).build()).get(5, TimeUnit.SECONDS);
            Map<String, byte[]> out = new LinkedHashMap<>();
            for (KeyValue kv : r.getKvs()) {
                out.put(kv.getKey().toString(StandardCharsets.UTF_8), kv.getValue().getBytes());
            }
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("前缀读失败: " + prefix, e);
        }
    }

    // ══════════ 事务/CAS(调度决策的根基) ══════════

    /**
     * 创建语义:key 不存在才写入(竞选/首注册/补 PENDING 分区)。
     * leaseId == null 表示持久键(分区归属等,不带租约)。
     */
    public boolean createIfAbsent(ByteSequence key, Object value, Long leaseId) {
        try {
            PutOption.Builder putOpt = PutOption.builder();
            if (leaseId != null) putOpt.withLeaseId(leaseId);
            TxnResponse r = client.getKVClient().txn()
                    .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0)))
                    .Then(Op.put(key, ByteSequence.from(Json.toBytes(value)), putOpt.build()))
                    .commit().get(5, TimeUnit.SECONDS);
            return r.isSucceeded();
        } catch (Exception e) {
            throw new IllegalStateException("createIfAbsent 失败: " + key, e);
        }
    }

    /**
     * CAS 条件写:当前 value 字节 == expected 才写入。
     * expected == null 表示"key 不存在"(createRevision == 0)。
     * 分配/迁移/交接全部走此方法,防陈旧主与并发写。
     */
    public boolean casPut(ByteSequence key, byte[] expected, Object newValue, Long leaseId) {
        try {
            Cmp cmp = (expected == null)
                    ? new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0))
                    : new Cmp(key, Cmp.Op.EQUAL, CmpTarget.value(ByteSequence.from(expected)));
            PutOption.Builder putOpt = PutOption.builder();
            if (leaseId != null) putOpt.withLeaseId(leaseId);
            TxnResponse r = client.getKVClient().txn()
                    .If(cmp)
                    .Then(Op.put(key, ByteSequence.from(Json.toBytes(newValue)), putOpt.build()))
                    .commit().get(5, TimeUnit.SECONDS);
            return r.isSucceeded();
        } catch (Exception e) {
            throw new IllegalStateException("casPut 失败: " + key, e);
        }
    }

    /** CAS 条件删除:当前 value 字节 == expected 才删除(优雅退出清理归属)。 */
    public boolean casDelete(ByteSequence key, byte[] expected) {
        try {
            Cmp cmp = (expected == null)
                    ? new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0))
                    : new Cmp(key, Cmp.Op.EQUAL, CmpTarget.value(ByteSequence.from(expected)));
            TxnResponse r = client.getKVClient().txn()
                    .If(cmp)
                    .Then(Op.delete(key, DeleteOption.DEFAULT))
                    .commit().get(5, TimeUnit.SECONDS);
            return r.isSucceeded();
        } catch (Exception e) {
            throw new IllegalStateException("casDelete 失败: " + key, e);
        }
    }

    // ══════════ watch ══════════

    /**
     * 前缀监听(带续传, 低危-5)。回调在 grpc 线程执行, 实现方需保证线程安全。
     *
     * <p>修复前 watch 不带 revision: 断线重连窗口内的事件会丢, 只能靠周期对账兜底;
     * 现在记录已见 revision, 断开后从 {@code lastRevision+1} 重建, 窗口内事件不再丢。
     * etcd 压缩了历史(CompactedException)时回退为不带 revision 监听(与旧行为一致)。
     */
    public Watch.Watcher watchPrefix(ByteSequence prefix, Listener l) {
        ResumableWatch w = new ResumableWatch(prefix, l);
        w.start();
        return w;
    }

    /** 续传 watch 包装: onNext 记录 revision; 断开后延迟 1s 从 lastRevision+1 重建(防双重建)。 */
    private final class ResumableWatch implements Watch.Watcher {
        private final ByteSequence prefix;
        private final Listener l;
        private final AtomicLong lastRev = new AtomicLong(0);
        private final AtomicBoolean compactedFallback = new AtomicBoolean(false);
        private final AtomicBoolean rebuilding = new AtomicBoolean(false);
        private volatile boolean closed = false;
        private volatile Watch.Watcher inner;

        ResumableWatch(ByteSequence prefix, Listener l) {
            this.prefix = prefix;
            this.l = l;
        }

        void start() {
            WatchOption.Builder b = WatchOption.builder().withPrefix(prefix);
            long rev = lastRev.get();
            if (rev > 0 && !compactedFallback.get()) {
                b.withRevision(rev + 1);
            }
            inner = client.getWatchClient().watch(prefix, b.build(), new Watch.Listener() {
                @Override public void onNext(WatchResponse r) {
                    long h = r.getHeader().getRevision();
                    if (h > 0) {
                        lastRev.accumulateAndGet(h, Math::max);
                    }
                    l.onEvent(r);
                }
                @Override public void onError(Throwable t) {
                    if (t instanceof CompactedException) {
                        compactedFallback.set(true);
                        log.warn("watch(prefix={}) 遇 compacted, 回退为不带 revision 监听(靠周期对账兜底)", prefix);
                    }
                    l.onError(t);
                    scheduleRebuild();
                }
                @Override public void onCompleted() {
                    l.onCompleted();
                    scheduleRebuild();
                }
            });
        }

        private void scheduleRebuild() {
            if (closed || !rebuilding.compareAndSet(false, true)) {
                return;
            }
            try {
                Thread.sleep(1000);   // 异常路径简单退避, 避免重连风暴
                if (closed) return;
                if (inner != null) inner.close();
                start();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                rebuilding.set(false);
            }
        }

        @Override public void close() {
            closed = true;
            if (inner != null) inner.close();
        }

        @Override public void requestProgress() {
            Watch.Watcher w = inner;
            if (w != null) w.requestProgress();
        }

        @Override public boolean isClosed() {
            Watch.Watcher w = inner;
            return closed || (w != null && w.isClosed());
        }
    }

    public interface Listener {
        void onEvent(WatchResponse r);
        default void onError(Throwable t) { log.warn("watch 出错: {}", t.getMessage()); }
        default void onCompleted() { /* 正常关闭/重连,静默 */ }
    }

    // ══════════ 竞选(每 ns 独立) ══════════

    /** 尝试当选(createIfAbsent leader 键)。失败=已有主。 */
    public boolean tryCampaign(ByteSequence leaderKey, Object self, long leaseId) {
        return createIfAbsent(leaderKey, self, leaseId);
    }

    /** 读当前 leader 值。 */
    public <T> Optional<T> getLeader(ByteSequence leaderKey, Class<T> type) {
        return getObj(leaderKey, type);
    }

    /** 监听单键(主挂时触发重新竞选)。 */
    public Watch.Watcher watchKey(ByteSequence key, Listener l) {
        return client.getWatchClient().watch(key, new Watch.Listener() {
            @Override public void onNext(WatchResponse r) { l.onEvent(r); }
            @Override public void onError(Throwable t) { l.onError(t); }
            @Override public void onCompleted() { l.onCompleted(); }
        });
    }
}

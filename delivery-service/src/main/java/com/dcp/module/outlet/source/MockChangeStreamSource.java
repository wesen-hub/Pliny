package com.dcp.module.outlet.source;

import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.ChangeEvent;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 骨架期数据源:内存模拟 L1 内部流(设计 §6.1 的可替换点)。
 *
 * <p>用途:在没有 TiDB/TiCDC 的环境里跑通**接口契约 + 位点语义 + 权限裁剪 + 批次/WATERMARK**,
 * 让出口服务与消费方 SDK 可以先开发、先联调。
 *
 * <p>与真实实现的差异(必须显式知道):
 * <ul>
 *   <li>快照返回的是**当前内存态**(真实实现必须 `AS OF TIMESTAMP T0` 读同一 T0 视图);</li>
 *   <li>无 canal-json 解析、无 Kafka 分区/ISR(由 {@code KafkaChangeStreamSource} 承担);</li>
 *   <li>watermark 直接取"已发出事件的最大 TSO",真实实现来自 TIDB_WATERMARK 事件。</li>
 * </ul>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "dcp.outlet.source", havingValue = "mock", matchIfMissing = true)
public class MockChangeStreamSource implements ChangeStreamSource {

    /** 每流事件(按 TSO 升序) */
    private final Map<String, List<ChangeEvent>> events = new ConcurrentHashMap<>();
    /** 每流当前态:业务主键 → 行(快照/回查用) */
    private final Map<String, LinkedHashMap<String, Map<String, Object>>> state = new ConcurrentHashMap<>();
    /** 每流起始 TSO(changefeed.start_ts 的等价物) */
    private final Map<String, Long> startTso = new ConcurrentHashMap<>();

    private final AtomicLong logical = new AtomicLong();
    private final AtomicLong logicalEpochCounter = new AtomicLong();
    private volatile int epoch = 1;
    private volatile int retentionHours = 168;

    private static final List<Object[]> DEFAULT_ROWS = List.of(
            new Object[]{"1", "alpha", "10.00", "CN", "13800000001"},
            new Object[]{"2", "beta", "20.00", "CN", "13800000002"},
            new Object[]{"3", "gamma", "30.00", "US", "13800000003"});

    @PostConstruct
    public void seed() {
        startTso.put("t_order", nextTso());
        for (Object[] r : DEFAULT_ROWS) {
            append("t_order", "INSERT", row(r), null);
        }
        log.info("[outlet] MockChangeStreamSource 已seed 流 t_order({} 条初始行, startTso={})",
                DEFAULT_ROWS.size(), startTso.get("t_order"));
    }

    /* ------------------------------------------------------------------ 模拟写入 */

    /** 模拟一次行变更(INSERT/UPDATE/DELETE),返回事件(已被采集) */
    public ChangeEvent append(String streamId, String op, Map<String, Object> row, Map<String, Object> before) {
        long tso = nextTso();
        ChangeEvent e = new ChangeEvent();
        e.setOp(op);
        e.setStreamId(streamId);
        e.setTso(tso);
        e.setEpoch(epoch);
        e.setTs(System.currentTimeMillis());
        Map<String, Object> key = new LinkedHashMap<>();
        if (row != null && row.get("id") != null) {
            key.put("id", row.get("id"));
        }
        e.setKey(key);
        e.setRow(row == null ? new LinkedHashMap<>() : new LinkedHashMap<>(row));
        e.setBefore(before == null ? null : new LinkedHashMap<>(before));

        events.computeIfAbsent(streamId, k -> Collections.synchronizedList(new ArrayList<>())).add(e);

        LinkedHashMap<String, Map<String, Object>> st = state.computeIfAbsent(streamId, k -> new LinkedHashMap<>());
        String pk = String.valueOf(key.getOrDefault("id", ""));
        if ("DELETE".equals(op)) {
            st.remove(pk);
        } else {
            st.put(pk, new LinkedHashMap<>(row));
        }
        return e;
    }

    private static Map<String, Object> row(Object[] r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r[0]);
        m.put("name", r[1]);
        m.put("amount", r[2]);
        m.put("region", r[3]);
        m.put("phone", r[4]);
        return m;
    }

    private long nextTso() {
        long l = logical.incrementAndGet() & 0x3FFFF;
        return (System.currentTimeMillis() << 18) | l;
    }

    /* -------------------------------------------------------------- ChangeStreamSource */

    @Override
    public StreamStatus status(String streamId) {
        StreamStatus s = new StreamStatus();
        s.setStreamId(streamId);
        s.setChangefeedId("chg-target-db");
        s.setEpoch(epoch);
        s.setEarliest(startTso.getOrDefault(streamId, 0L));
        s.setLatest(latestTso(streamId));
        s.setWatermarkTs(watermark(streamId));
        s.setRetentionHours(retentionHours);
        return s;
    }

    @Override
    public Long watermark(String streamId) {
        List<ChangeEvent> list = events.get(streamId);
        if (list == null || list.isEmpty()) {
            return null;
        }
        synchronized (list) {
            return list.get(list.size() - 1).getTso();
        }
    }

    @Override
    public ChangeBatch changes(String streamId, long afterTso, int epoch, int limit) {
        List<ChangeEvent> list = events.get(streamId);
        ChangeBatch b = new ChangeBatch();
        b.setStreamId(streamId);
        b.setEpoch(epoch);
        b.setScannedFrom(afterTso);
        b.setWatermarkTs(watermark(streamId));
        if (list == null || list.isEmpty()) {
            b.setScannedTo(afterTso);
            b.setNextPosition(com.dcp.module.outlet.model.PositionKey.of(epoch, afterTso));
            return b;
        }
        List<ChangeEvent> snapshot;
        synchronized (list) {
            snapshot = new ArrayList<>(list);
        }
        List<ChangeEvent> picked = new ArrayList<>();
        long last = afterTso;
        for (ChangeEvent e : snapshot) {
            if (e.getTso() > afterTso && picked.size() < limit) {
                picked.add(e);
                last = e.getTso();
            }
        }
        b.setEvents(picked);
        b.setScannedTo(last);
        b.setHasMore(picked.size() == limit);
        b.setNextPosition(com.dcp.module.outlet.model.PositionKey.of(epoch, last));
        return b;
    }

    @Override
    public SnapshotPage snapshot(String streamId, long boundaryTso, int epoch, String pageToken, int limit) {
        SnapshotPage page = new SnapshotPage();
        page.setStreamId(streamId);
        page.setBoundaryTso(boundaryTso);
        page.setEpoch(epoch);
        List<Map<String, Object>> rows = new ArrayList<>(state.getOrDefault(streamId, new LinkedHashMap<>()).values());
        int from = pageToken == null ? 0 : Integer.parseInt(pageToken);
        int to = Math.min(from + limit, rows.size());
        page.setRows(new ArrayList<>(rows.subList(Math.min(from, rows.size()), to)));
        page.setRowsEstimate((long) rows.size());
        page.setRowsExported(to);
        page.setDone(to >= rows.size());
        page.setPageToken(page.isDone() ? null : String.valueOf(to));
        // 说明:骨架实现返回"当前态";真实实现必须是 AS OF TIMESTAMP boundaryTso 的同一视图
        return page;
    }

    @Override
    public long estimateRows(String streamId, long boundaryTso) {
        java.util.Map<?, ?> m = state.get(streamId);
        return m == null ? 0L : m.size();
    }

    @Override
    public long positionAt(String streamId, long timestampMs) {
        long candidate = (timestampMs << 18);
        List<ChangeEvent> list = events.get(streamId);
        if (list == null || list.isEmpty()) {
            return candidate;
        }
        synchronized (list) {
            for (ChangeEvent e : list) {
                if (e.getTso() >= candidate) {
                    return e.getTso();
                }
            }
            return list.get(list.size() - 1).getTso();
        }
    }

    @Override
    public Map<String, Object> keyLookup(String streamId, Map<String, Object> bizKey, Long asOfTso) {
        String pk = String.valueOf(bizKey.getOrDefault("id", ""));
        Map<String, Object> hit = state.getOrDefault(streamId, new LinkedHashMap<>()).get(pk);
        // 说明:骨架实现忽略 asOfTso;真实实现必须 AS OF TIMESTAMP <asOfTso>(否则会读到"未来值")
        return hit == null ? null : new LinkedHashMap<>(hit);
    }

    @Override
    public Set<String> availableColumns(String streamId) {
        LinkedHashMap<String, Map<String, Object>> st = state.get(streamId);
        if (st == null || st.isEmpty()) {
            return Set.of("id", "name", "amount", "region", "phone");
        }
        return new LinkedHashSet<>(st.values().iterator().next().keySet());
    }

    private long latestTso(String streamId) {        List<ChangeEvent> list = events.get(streamId);
        long latest = nextTso();
        if (list != null && !list.isEmpty()) {
            synchronized (list) {
                latest = Math.max(latest, list.get(list.size() - 1).getTso());
            }
        }
        return latest;
    }

    /** 模拟 epoch 变更(重建 changefeed / topic 重建);用于验证 EPOCH_MISMATCH 拦截 */
    public int bumpEpoch() {
        return ++epoch;
    }

    public long logicalEpoch() {
        return logicalEpochCounter.incrementAndGet();
    }
}

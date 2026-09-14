package com.dcp.module.outlet.source;

import com.dcp.module.outlet.OutletProperties;
import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.ChangeEvent;
import com.dcp.module.outlet.model.PositionKey;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * L1 内部流数据源(设计 §6.1 的替换点 / §5.5 / §6.3)。
 *
 * <p>开启方式:{@code dcp.outlet.source=kafka}(并配好 {@code dcp.outlet.kafka.*} 与 {@code dcp.outlet.tidb.*})。
 *
 * <p>职责:
 * <ul>
 *   <li><b>增量</b>:{@link KafkaListener} 消费 TiCDC 投递的 canal-json(含 WATERMARK),解析成 {@link ChangeEvent},
 *       进内存环形缓冲(骨架期;生产应改为按位点 seek 读 Kafka,见 README 待建项);</li>
 *   <li><b>快照</b>:走数据面只读入口,`AS OF TIMESTAMP` 同一 T0 视图 + 主键游标分页(设计 §6.5);
 *       <b>禁止回表式逐条点查</b>(设计 §6.6);</li>
 *   <li><b>回查</b>:按主键 + asOf 位点回查(`AS OF TIMESTAMP`),仅作对账/补漏兜底。</li>
 * </ul>
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "dcp.outlet.source", havingValue = "kafka")
public class KafkaChangeStreamSource implements ChangeStreamSource {

    private final OutletProperties props;
    private final CanalJsonParser parser;

    /** 每流事件缓冲(按 TSO 升序) */
    private final Map<String, ArrayDeque<ChangeEvent>> buffers = new ConcurrentHashMap<>();
    /** 每流 watermark(最近一次 TIDB_WATERMARK) */
    private final Map<String, Long> watermarks = new ConcurrentHashMap<>();
    /** 每流首次见到的最小 TSO(用于 earliest 收敛) */
    private final Map<String, Long> firstSeen = new ConcurrentHashMap<>();
    /** 列信息缓存 */
    private final Set<String> columnCache = ConcurrentHashMap.newKeySet();

    private volatile JdbcTemplate readJdbc;

    public KafkaChangeStreamSource(@Autowired OutletProperties props,
                                   @Autowired CanalJsonParser parser) {
        this.props = props;
        this.parser = parser;
    }

    @PostConstruct
    public void init() {
        if (props.getTidb() != null && props.getTidb().getUrl() != null && !props.getTidb().getUrl().isBlank()) {
            DriverManagerDataSource ds = new DriverManagerDataSource(
                    props.getTidb().getUrl(), props.getTidb().getUsername(), props.getTidb().getPassword());
            this.readJdbc = new JdbcTemplate(ds);
            log.info("[outlet] Kafka 数据源已启用,快照只读入口={}", props.getTidb().getUrl());
        } else {
            log.warn("[outlet] Kafka 数据源已启用,但未配置 dcp.outlet.tidb.url → 快照/回查接口将报错(增量不受影响)");
        }
        log.info("[outlet] 订阅 L1 流: topics={} group={} startTso={}",
                props.getKafka().getTopics(), props.getKafka().getGroupId(), props.getKafka().getStartTso());
    }

    /* ------------------------------------------------------------------ Kafka 消费 */

    @KafkaListener(topics = "#{'${dcp.outlet.kafka.topics:dcp.cdc.target_db}'.split(',')}",
            groupId = "${dcp.outlet.kafka.group-id:dcp-outlet}",
            autoStartup = "true")
    public void onMessage(String message) {
        CanalJsonParser.Parsed p = parser.parse(message);
        switch (p.type()) {
            case WATERMARK -> {
                if (p.watermarkTs() != null) {
                    // 一个 changefeed / 一个 topic 只有一条水位线;watermark() 单流缺省回退到它
                    watermarks.put("__global__", p.watermarkTs());
                }
            }
            case ROW -> {
                for (ChangeEvent e : p.events()) {
                    ArrayDeque<ChangeEvent> buf = buffers.computeIfAbsent(e.getStreamId(), k -> new ArrayDeque<>());
                    synchronized (buf) {
                        buf.addLast(e);
                        firstSeen.putIfAbsent(e.getStreamId(), e.getTso());
                        // 骨架期内存缓冲上限;生产改为"按位点 seek 从 Kafka 重读",不存在上限问题
                        while (buf.size() > props.getKafka().getBufferMaxEvents()) {
                            buf.pollFirst();
                        }
                    }
                }
            }
            case DDL -> log.info("[outlet] 收到 DDL 事件(当前仅记录):{}", p.ddlSql());
            default -> { /* ignored */ }
        }
    }

    /* ------------------------------------------------------------ ChangeStreamSource */

    @Override
    public StreamStatus status(String streamId) {
        StreamStatus s = new StreamStatus();
        s.setStreamId(streamId);
        s.setChangefeedId("chg-target-db");
        s.setEpoch(props.getEpoch());
        long start = props.getKafka().getStartTso();
        long seen = firstSeen.getOrDefault(streamId, start);
        // earliest = max(start_ts, 缓冲里最早事件)——生产实现再与 GC safe point 取大
        s.setEarliest(Math.max(start, seen));
        s.setLatest(latestTso(streamId));
        s.setWatermarkTs(watermark(streamId));
        s.setRetentionHours(props.getRetentionHours());
        return s;
    }

    @Override
    public Long watermark(String streamId) {
        Long w = watermarks.get(streamId);
        return w != null ? w : watermarks.get("__global__");
    }

    @Override
    public ChangeBatch changes(String streamId, long afterTso, int epoch, int limit) {
        ChangeBatch b = new ChangeBatch();
        b.setStreamId(streamId);
        b.setEpoch(epoch);
        b.setScannedFrom(afterTso);
        b.setWatermarkTs(watermark(streamId));
        ArrayDeque<ChangeEvent> buf = buffers.get(streamId);
        long last = afterTso;
        if (buf != null) {
            synchronized (buf) {
                for (ChangeEvent e : buf) {
                    if (e.getTso() > afterTso && b.getEvents().size() < limit) {
                        e.setEpoch(epoch);
                        b.getEvents().add(e);
                        last = e.getTso();
                    }
                }
            }
        }
        b.setScannedTo(last);
        b.setHasMore(b.getEvents().size() == limit);
        b.setNextPosition(PositionKey.of(epoch, last));
        return b;
    }

    @Override
    public SnapshotPage snapshot(String streamId, long boundaryTso, int epoch, String pageToken, int limit) {
        requireRead();
        SnapshotPage page = new SnapshotPage();
        page.setStreamId(streamId);
        page.setBoundaryTso(boundaryTso);
        page.setEpoch(epoch);

        String pk = props.getTidb().getPkColumn();
        String snapshotClause = " AS OF TIMESTAMP '" + toDatetime(boundaryTso) + "'";
        String cursor = pageToken == null || pageToken.isBlank() ? null : pageToken;

        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(streamId).append(snapshotClause);
        List<Object> args = new ArrayList<>();
        if (cursor != null) {
            sql.append(" WHERE ").append(pk).append(" > ?");
            args.add(cursor);
        }
        sql.append(" ORDER BY ").append(pk).append(" LIMIT ?");
        args.add(limit);

        List<Map<String, Object>> rows = readJdbc.queryForList(sql.toString(), args.toArray());
        page.setRows(rows);
        page.setRowsExported(rows.size());
        boolean done = rows.size() < limit;
        page.setDone(done);
        if (!done) {
            Object lastPk = rows.get(rows.size() - 1).get(pk);
            page.setPageToken(String.valueOf(lastPk));
        }
        try {
            Long total = readJdbc.queryForObject("SELECT COUNT(*) FROM " + streamId + snapshotClause, Long.class);
            page.setRowsEstimate(total);
        } catch (Exception ignore) {
            // 计数失败不影响分页
        }
        return page;
    }

    @Override
    public long positionAt(String streamId, long timestampMs) {
        // TSO 物理部分是毫秒;+1 保证落在该时刻之后
        return (timestampMs << 18) + 1;
    }

    @Override
    public Map<String, Object> keyLookup(String streamId, Map<String, Object> bizKey, Long asOfTso) {
        requireRead();
        String pk = props.getTidb().getPkColumn();
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(streamId);
        if (asOfTso != null) {
            // ★ 必须按位点回查:否则读到的是"未来值",会把未来状态混进历史批次(设计 §6.6)
            sql.append(" AS OF TIMESTAMP '").append(toDatetime(asOfTso)).append("'");
        }
        sql.append(" WHERE ").append(pk).append(" = ? LIMIT 1");
        List<Map<String, Object>> rows = readJdbc.queryForList(sql.toString(), bizKey.getOrDefault(pk, bizKey.values().iterator().next()));
        return rows.isEmpty() ? null : rows.get(0);
    }

    @Override
    public Set<String> availableColumns(String streamId) {
        if (readJdbc == null) {
            return null;
        }
        String cacheKey = "cols::" + streamId;
        if (columnCache.contains(cacheKey)) {
            Set<String> cols = new LinkedHashSet<>();
            try {
                readJdbc.queryForList(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
                        streamId).forEach(r -> cols.add(String.valueOf(r.get("column_name"))));
            } catch (Exception e) {
                return null;
            }
            return cols;
        }
        try {
            Set<String> cols = new LinkedHashSet<>();
            readJdbc.queryForList(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
                    streamId).forEach(r -> cols.add(String.valueOf(r.get("column_name"))));
            if (!cols.isEmpty()) {
                columnCache.add(cacheKey);
                return cols;
            }
        } catch (Exception e) {
            log.debug("[outlet] 列信息查询失败:{}", e.getMessage());
        }
        return null;
    }

    /* ------------------------------------------------------------------ helpers */

    private void requireRead() {
        if (readJdbc == null) {
            throw new com.dcp.common.BizException(com.dcp.module.outlet.OutletErrorCode.OUTLET_INTERNAL,
                    "快照/回查需要配置 dcp.outlet.tidb.url(数据面只读入口)");
        }
    }

    private long latestTso(String streamId) {
        ArrayDeque<ChangeEvent> buf = buffers.get(streamId);
        long latest = Math.max(props.getKafka().getStartTso(), System.currentTimeMillis() << 18);
        if (buf != null) {
            synchronized (buf) {
                ChangeEvent last = buf.peekLast();
                if (last != null) {
                    latest = Math.max(latest, last.getTso());
                }
            }
        }
        return latest;
    }

    /** TSO → 时间字符串(TSO 物理部分 = 毫秒),用于 AS OF TIMESTAMP 字面量 */
    private String toDatetime(long tso) {
        long ms = tso >> 18;
        LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(ms), ZoneId.systemDefault());
        return dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /** 调试:当前缓冲情况 */
    public Map<String, Object> bufferStats() {
        Map<String, Object> m = new LinkedHashMap<>();
        buffers.forEach((k, v) -> m.put(k, v.size()));
        m.put("watermarks", watermarks);
        return m;
    }
}

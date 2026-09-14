package com.dcp.module.outlet;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.dcp.common.BizException;
import com.dcp.module.outlet.entity.SnapshotJobEntity;
import com.dcp.module.outlet.mapper.SnapshotJobMapper;
import com.dcp.module.outlet.metrics.OutletMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 快照任务登记与 TTL(设计 §6.5)。
 *
 * <p>解决的问题:大表快照是"分钟~小时级"的长任务,必须
 * ①**可中断重入**(同一 boundary 的页序列稳定)②**能登记**(boundary 与 TTL 落库)
 * ③**能作废**(超过 TTL / GC 窗口后不可再续,必须用新 T0 重来)。
 *
 * <p>两种模式:元库(注入 {@link SnapshotJobMapper})或内存(骨架/单测)。
 */
@Slf4j
@Service
public class SnapshotJobService {

    private final SnapshotJobMapper mapper;
    private final OutletProperties props;
    /** 指标登记(可为 null:单测/无监控场景 → 跳过埋点) */
    private final OutletMetrics metrics;

    /** 内存模式:{consumer::stream::boundary → job} */
    private final Map<String, SnapshotJobEntity> mem = new ConcurrentHashMap<>();
    private final AtomicLong seq = new AtomicLong();

    public SnapshotJobService(SnapshotJobMapper mapper, OutletProperties props) {
        this(mapper, props, null);
    }

    @Autowired
    public SnapshotJobService(@Autowired(required = false) SnapshotJobMapper mapper,
                              @Autowired(required = false) OutletProperties props,
                              @Autowired(required = false) OutletMetrics metrics) {
        this.mapper = mapper;
        this.props = props == null ? new OutletProperties() : props;
        this.metrics = metrics;
    }

    /** 首页调用:登记任务(READY → PAGING)。同一 boundary 重复发起时复用已有任务。 */
    public SnapshotJobEntity register(String consumerId, String streamId, String contractId, long boundaryTso) {
        SnapshotJobEntity exist = find(consumerId, streamId, boundaryTso);
        if (exist != null && !expired(exist)) {
            exist.setStatus(SnapshotJobEntity.STATUS_PAGING);
            save(exist);
            return exist;
        }
        SnapshotJobEntity job = new SnapshotJobEntity();
        job.setConsumerId(consumerId);
        job.setStreamId(streamId);
        job.setContractId(contractId);
        job.setBoundaryTso(boundaryTso);
        job.setStatus(SnapshotJobEntity.STATUS_PAGING);
        job.setRowsExported(0L);
        job.setExpireAt(LocalDateTime.now().plusHours(ttlHours()));
        job.setCreateTime(LocalDateTime.now());
        job.setUpdateTime(LocalDateTime.now());
        save(job);
        snapshotMetric(consumerId, streamId, SnapshotJobEntity.STATUS_PAGING);
        log.info("[outlet] 登记快照任务 consumer={} stream={} boundary={} ttl={}h",
                consumerId, streamId, boundaryTso, ttlHours());
        return job;
    }

    public SnapshotJobEntity find(String consumerId, String streamId, long boundaryTso) {
        if (useMapper()) {
            return mapper.selectOne(new QueryWrapper<SnapshotJobEntity>()
                    .eq("consumer_id", consumerId)
                    .eq("stream_id", streamId)
                    .eq("boundary_tso", boundaryTso)
                    .last("limit 1"));
        }
        return mem.get(key(consumerId, streamId, boundaryTso));
    }

    /** 续拉前校验:过期即失败(必须用新 T0 重来),绝不返回半截错数据 */
    public void assertUsable(SnapshotJobEntity job) {
        if (job == null) {
            throw new BizException(OutletErrorCode.SNAPSHOT_JOB_NOT_FOUND,
                    OutletErrorCode.message(OutletErrorCode.SNAPSHOT_JOB_NOT_FOUND));
        }
        if (expired(job)) {
            job.setStatus(SnapshotJobEntity.STATUS_EXPIRED);
            save(job);
            snapshotMetric(job.getConsumerId(), job.getStreamId(), SnapshotJobEntity.STATUS_EXPIRED);
            throw new BizException(OutletErrorCode.SNAPSHOT_JOB_EXPIRED,
                    "boundary=" + job.getBoundaryTso() + " 已过期(" + job.getExpireAt() + ")");
        }
    }

    public boolean expired(SnapshotJobEntity job) {
        return job != null && job.getExpireAt() != null && job.getExpireAt().isBefore(LocalDateTime.now());
    }

    /** 页推进:记录游标与已导出行数 */
    public void advance(SnapshotJobEntity job, String cursor, String pageToken, long rowsExported, Long rowsEstimate) {
        if (job == null) {
            return;
        }
        job.setCursorPk(cursor);
        job.setPageToken(pageToken);
        job.setRowsExported(rowsExported);
        if (rowsEstimate != null) {
            job.setRowsEstimate(rowsEstimate);
        }
        job.setStatus(SnapshotJobEntity.STATUS_PAGING);
        job.setUpdateTime(LocalDateTime.now());
        save(job);
    }

    public void complete(SnapshotJobEntity job) {
        if (job == null) {
            return;
        }
        job.setStatus(SnapshotJobEntity.STATUS_DONE);
        job.setPageToken(null);
        job.setUpdateTime(LocalDateTime.now());
        save(job);
        snapshotMetric(job.getConsumerId(), job.getStreamId(), SnapshotJobEntity.STATUS_DONE);
        log.info("[outlet] 快照任务完成 consumer={} stream={} boundary={} rows={}",
                job.getConsumerId(), job.getStreamId(), job.getBoundaryTso(), job.getRowsExported());
    }

    /**
     * 某消费方的快照任务**当前存量**(按状态)—— 消费任务详情页直接读,不依赖 Prometheus。
     * 与 {@code outlet_snapshot_job_total} 的区别:后者是「状态迁移累计次数」(counter),
     * 前者是「此刻有多少个任务处于该状态」(盘点存量)。
     */
    public Map<String, Long> countsByStatus(String consumerId) {
        Map<String, Long> counts = new LinkedHashMap<>();
        if (useMapper()) {
            List<SnapshotJobEntity> rows = mapper.selectList(new QueryWrapper<SnapshotJobEntity>()
                    .eq("consumer_id", consumerId).select("status"));
            for (SnapshotJobEntity r : rows) {
                String st = r.getStatus() == null ? "UNKNOWN" : r.getStatus();
                counts.merge(st, 1L, Long::sum);
            }
            return counts;
        }
        for (SnapshotJobEntity job : mem.values()) {
            if (consumerId.equals(job.getConsumerId())) {
                String st = job.getStatus() == null ? "UNKNOWN" : job.getStatus();
                counts.merge(st, 1L, Long::sum);
            }
        }
        return counts;
    }

    private void snapshotMetric(String consumerId, String streamId, String status) {
        if (metrics != null) {
            metrics.recordSnapshotJob(consumerId, streamId, status);
        }
    }

    /** 调试/观测 */
    public Map<String, Object> statOf(String consumerId, String streamId, long boundaryTso) {
        SnapshotJobEntity job = find(consumerId, streamId, boundaryTso);
        if (job == null) {
            return Map.of("found", false);
        }
        return Map.of("found", true, "status", String.valueOf(job.getStatus()),
                "rowsExported", job.getRowsExported() == null ? 0L : job.getRowsExported(),
                "expireAt", String.valueOf(job.getExpireAt()));
    }

    /* ------------------------------------------------------------------ private */

    private int ttlHours() {
        int ttl = props.getSnapshot() == null ? 24 : props.getSnapshot().getTtlHours();
        return ttl <= 0 ? 0 : ttl;
    }

    private boolean useMapper() {
        return mapper != null;
    }

    private void save(SnapshotJobEntity job) {
        if (useMapper()) {
            if (job.getId() == null) {
                mapper.insert(job);
            } else {
                mapper.updateById(job);
            }
            return;
        }
        if (job.getId() == null) {
            job.setId(seq.incrementAndGet());
        }
        mem.put(key(job.getConsumerId(), job.getStreamId(), job.getBoundaryTso()), job);
    }

    private String key(String consumerId, String streamId, long boundaryTso) {
        return consumerId + "::" + streamId + "::" + boundaryTso;
    }
}

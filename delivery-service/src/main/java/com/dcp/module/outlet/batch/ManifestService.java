package com.dcp.module.outlet.batch;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.dcp.module.outlet.entity.SnapshotManifestEntity;
import com.dcp.module.outlet.mapper.SnapshotManifestMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 物化清单服务(设计 §6.3 技术要点 4/5、§6.4.1 衔接契约 3)。
 *
 * <p>作用:把"批产物是否可见"变成一次**原子发布**——产物先写不可变 staging,
 * 校验通过后再把 manifest 置 {@code PUBLISHED};**位点只能在这之后提交**。
 */
@Slf4j
@Service
public class ManifestService {

    private final SnapshotManifestMapper mapper;
    /** 内存模式:{manifestId → manifest} */
    private final Map<String, SnapshotManifestEntity> mem = new ConcurrentHashMap<>();

    public ManifestService(@Autowired(required = false) SnapshotManifestMapper mapper) {
        this.mapper = mapper;
    }

    /** 幂等键:同 consumer/stream/jobType/边界(或区间)复用同一条 manifest */
    public static String manifestId(String consumerId, String streamId, String jobType, Long boundaryTso,
                                    Long loTso, Long hiTso) {
        String range = boundaryTso != null ? String.valueOf(boundaryTso) : (loTso + "-" + hiTso);
        return consumerId + ":" + streamId + ":" + jobType + ":" + range;
    }

    public SnapshotManifestEntity start(String consumerId, String streamId, String jobType,
                                        Long boundaryTso, Long loTso, Long hiTso, int epoch) {
        SnapshotManifestEntity m = new SnapshotManifestEntity();
        m.setManifestId(manifestId(consumerId, streamId, jobType, boundaryTso, loTso, hiTso));
        m.setConsumerId(consumerId);
        m.setStreamId(streamId);
        m.setJobType(jobType);
        m.setBoundaryTso(boundaryTso);
        m.setLoTso(loTso);
        m.setHiTso(hiTso);
        m.setStatus(SnapshotManifestEntity.STATUS_RUNNING);
        m.setEpoch(epoch);
        m.setCreateTime(LocalDateTime.now());
        save(m);
        return m;
    }

    /** 已发布且可复用的产物(重跑幂等依据) */
    public SnapshotManifestEntity findPublished(String consumerId, String streamId, String jobType,
                                                Long boundaryTso, Long loTso, Long hiTso) {
        SnapshotManifestEntity m = get(manifestId(consumerId, streamId, jobType, boundaryTso, loTso, hiTso));
        return m != null && SnapshotManifestEntity.STATUS_PUBLISHED.equals(m.getStatus()) ? m : null;
    }

    public SnapshotManifestEntity get(String manifestId) {
        if (mapper != null) {
            return mapper.selectById(manifestId);
        }
        return mem.get(manifestId);
    }

    /** ★ 原子发布:此后产物才对消费方可见,位点才能提交 */
    public void publish(SnapshotManifestEntity m, ExecutionResult result) {
        m.setStagingPath(result.getStagingPath());
        m.setRowsWritten(result.getRowsWritten());
        m.setChecksum(result.getChecksum());
        m.setStatus(SnapshotManifestEntity.STATUS_PUBLISHED);
        m.setPublishTime(LocalDateTime.now());
        save(m);
        log.info("[outlet-batch] manifest 已发布 id={} rows={} staging={}",
                m.getManifestId(), m.getRowsWritten(), m.getStagingPath());
    }

    public void fail(SnapshotManifestEntity m, String error) {
        m.setStatus(SnapshotManifestEntity.STATUS_FAILED);
        // 错误文本截断:checksum 列只有 varchar(128),超长会反过来把原始异常盖掉(实测坑)
        String detail = error == null ? "unknown" : error;
        String brief = ("error:" + detail).length() > 120 ? ("error:" + detail).substring(0, 120) : "error:" + detail;
        m.setChecksum(brief);
        save(m);
        log.warn("[outlet-batch] manifest 失败 id={} err={}", m.getManifestId(), detail);
    }

    private void save(SnapshotManifestEntity m) {
        if (mapper != null) {
            if (get(m.getManifestId()) == null) {
                mapper.insert(m);
            } else {
                mapper.updateById(m);
            }
            return;
        }
        mem.put(m.getManifestId(), m);
    }
}

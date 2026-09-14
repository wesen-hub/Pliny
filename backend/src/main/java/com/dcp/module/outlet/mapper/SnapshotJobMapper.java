package com.dcp.module.outlet.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.outlet.entity.SnapshotJobEntity;
import org.apache.ibatis.annotations.Mapper;

/** 快照任务 Mapper(DDL 的 snapshot_job 表;唯一键 consumer_id+stream_id+boundary_tso) */
@Mapper
public interface SnapshotJobMapper extends BaseMapper<SnapshotJobEntity> {
}

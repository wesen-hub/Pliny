package com.dcp.module.outlet.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.outlet.entity.SnapshotManifestEntity;
import org.apache.ibatis.annotations.Mapper;

/** 物化清单 Mapper(DDL 的 snapshot_manifest 表) */
@Mapper
public interface SnapshotManifestMapper extends BaseMapper<SnapshotManifestEntity> {
}

package com.perm.pdp.cache;

import com.perm.pdp.dto.ArtifactResponseDto;

import java.util.Optional;

/**
 * L2 产物缓存（配送链第二层：PDP 进程外共享缓存，多副本 PDP 命中复用）
 * 键含 planVersion（键空间版本化：策略发布后旧键自然淘汰）。
 * 值仅存模板无值产物（未签名，签名属响应边界动作；与"缓存值仅存模板"设计一致）。
 */
public interface L2ArtifactCache {

    Optional<ArtifactResponseDto> get(String key);

    void put(String key, ArtifactResponseDto dto);
}

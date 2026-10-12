package com.perm.pdp.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.perm.pdp.dto.ArtifactResponseDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

/**
 * Redis L2 产物缓存（perm.redis.enabled=true 时启用）
 * 键：perm:artifact:{planVersion#resourceId#action#指纹}（版本化键空间，发布后自然失效）
 * 值：模板无值产物 JSON（未签名）；TTL 兜底淘汰。
 * 降级：Redis 不可达时读写均失败返回空（回源判定链），不阻断判定。
 */
@Component
@ConditionalOnProperty(name = "perm.redis.enabled", havingValue = "true")
public class RedisL2ArtifactCache implements L2ArtifactCache {

    private static final Logger log = LoggerFactory.getLogger(RedisL2ArtifactCache.class);
    private static final String KEY_PREFIX = "perm:artifact:";

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Duration ttl;

    public RedisL2ArtifactCache(StringRedisTemplate redis,
                                @Value("${perm.redis.ttl-seconds:60}") long ttlSeconds) {
        this.redis = redis;
        this.ttl = Duration.ofSeconds(ttlSeconds);
    }

    @Override
    public Optional<ArtifactResponseDto> get(String key) {
        try {
            String json = redis.opsForValue().get(KEY_PREFIX + key);
            if (json == null) {
                return Optional.empty();
            }
            return Optional.of(mapper.readValue(json, ArtifactResponseDto.class));
        } catch (Exception e) {
            log.warn("[L2] Redis 读取失败（回源判定链）: {}", e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public void put(String key, ArtifactResponseDto dto) {
        try {
            redis.opsForValue().set(KEY_PREFIX + key, mapper.writeValueAsString(dto), ttl);
        } catch (Exception e) {
            log.warn("[L2] Redis 写入失败（不影响判定）: {}", e.getMessage());
        }
    }
}

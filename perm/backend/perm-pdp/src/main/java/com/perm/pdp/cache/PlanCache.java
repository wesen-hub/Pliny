package com.perm.pdp.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.perm.pdp.dto.ArtifactResponseDto;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * L1 判定缓存（Caffeine + 单飞）
 * 键 = planVersion#resourceId#action#指纹（缓存键三段式，R-K）
 * 单飞（SingleFlight）：并发相同键只执行一次判定，其余共享同一 CompletableFuture（Caffeine 官方模式）。
 * 说明：L2 Redis（键空间版本化 + 指针前移）为正式部署形态，当前演示级只实现 L1。
 */
@Component
public class PlanCache {

    private final com.github.benmanes.caffeine.cache.Cache<String, CompletableFuture<ArtifactResponseDto>> cache;
    private final ExecutorService computePool = Executors.newFixedThreadPool(4);

    public PlanCache() {
        this.cache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofSeconds(30))
                .build();
    }

    /** 缓存读取 + 单飞计算（computeIfAbsent 原子性保证同键单次计算） */
    public CompletableFuture<ArtifactResponseDto> get(String key,
                                                      java.util.function.Supplier<ArtifactResponseDto> compute) {
        return cache.get(key, k ->
                CompletableFuture.supplyAsync(compute, computePool));
    }

    /** 失效（版本热更新后清空，键空间含 planVersion 本可自然失效，防御性清空） */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    public long size() {
        return cache.estimatedSize();
    }
}

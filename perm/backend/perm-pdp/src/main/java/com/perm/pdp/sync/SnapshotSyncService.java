package com.perm.pdp.sync;

import com.perm.pdp.cache.PlanCache;
import com.perm.pdp.policy.PolicySnapshotLoader;
import com.perm.common.compile.PolicyCompiler;
import com.perm.common.model.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

/**
 * PDP 快照同步（版本比对兜底通道）
 * 后台定时 5s 拉取管理面快照比对版本：落后 → 编译 DSL → 安装快照 → 清空 L1 缓存。
 * 正式形态双通道：Kafka 事件驱动（主）+ 本服务版本比对（兜底）；
 * 事件仅触发失效不预编译，新版本首次命中时按需编译。
 * 降级：管理面不可达/拉取失败时保持本地快照继续服务（不中断判定链）。
 */
public class SnapshotSyncService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotSyncService.class);

    private final PolicySnapshotLoader snapshotLoader;
    private final PlanCache planCache;
    private final PolicyCompiler compiler;
    private final SnapshotFetcher fetcher;

    public SnapshotSyncService(PolicySnapshotLoader snapshotLoader,
                               PlanCache planCache,
                               PolicyCompiler compiler,
                               SnapshotFetcher fetcher) {
        this.snapshotLoader = snapshotLoader;
        this.planCache = planCache;
        this.compiler = compiler;
        this.fetcher = fetcher;
    }

    /** 定时同步（5s 版本比对，R-Sync） */
    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void sync() {
        try {
            AdminSnapshotDto remote = fetcher.fetch();
            int localVersion = snapshotLoader.current().version();
            if (remote.getVersion() <= localVersion) {
                return; // 版本无变化，静默跳过
            }
            List<Policy> policies = remote.getPolicies() == null ? List.of()
                    : remote.getPolicies().stream().map(compiler::compile).toList();
            if (snapshotLoader.install(policies, remote.getVersion())) {
                planCache.invalidateAll(); // 缓存键含 planVersion 可自然失效，防御性清空
                log.info("[同步] 快照升级 v{} → v{}，策略 {} 条（L1 缓存已失效）",
                        localVersion, remote.getVersion(), policies.size());
            }
        } catch (Exception e) {
            log.warn("[同步] 管理面不可达或快照拉取失败，保持本地快照 v{} 继续服务（降级）: {}",
                    snapshotLoader.current().version(), e.getMessage());
        }
    }
}

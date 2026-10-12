package com.perm.pdp.sync;

/**
 * 快照拉取器（可测性抽象：测试注入 fake，生产用 RestSnapshotFetcher）
 */
public interface SnapshotFetcher {

    /** 拉取管理面当前快照；不可达时抛异常（由调用方降级处理） */
    AdminSnapshotDto fetch();
}

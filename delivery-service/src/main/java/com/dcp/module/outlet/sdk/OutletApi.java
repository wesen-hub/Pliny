package com.dcp.module.outlet.sdk;

import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;

import java.util.Map;

/**
 * 出口 L3 接口的客户端抽象(设计 §6.9)。
 *
 * <p>SDK 对上游只依赖本接口:{@link HttpOutletApi} 是真实实现;测试用内存假实现。
 * 消费方**不感知**快照段/增量段的差异 —— 由 {@link OutletConsumer} 内部路由。
 */
public interface OutletApi {

    StreamStatus streamStatus(String streamId);

    SnapshotPage snapshot(String streamId, Long boundaryTso, String pageToken, int limit);

    ChangeBatch changes(String streamId, String afterPosition, int limit);

    Map<String, Object> keyLookup(String streamId, String bizKey, Long asOfTso);

    Map<String, Object> positionAt(String streamId, long timeMs);

    Map<String, Object> commit(String streamId, String position);
}

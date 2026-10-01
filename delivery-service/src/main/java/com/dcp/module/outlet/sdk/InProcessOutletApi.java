package com.dcp.module.outlet.sdk;

import com.dcp.module.outlet.ChangeOutletService;
import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;

import java.util.Map;

/**
 * 出口 L3 接口的**进程内实现**:直接调 {@link ChangeOutletService},不经过 HTTP。
 *
 * <p>用途:平台内部消费方(打标/清洗/周期批作业)走同一条出口能力,但不必自调 HTTP 接口。
 * 语义与 {@link HttpOutletApi} 完全一致(同一套权限/配额/审计/位点)。
 *
 * <p>注意:**消费方身份在构造时绑定**(HTTP 版通过 token/query 携带),因此一个实例只服务一个消费方。
 */
public class InProcessOutletApi implements OutletApi {

    private final ChangeOutletService service;
    private final String consumerId;

    public InProcessOutletApi(ChangeOutletService service, String consumerId) {
        this.service = service;
        this.consumerId = consumerId;
    }

    public String consumerId() {
        return consumerId;
    }

    @Override
    public StreamStatus streamStatus(String streamId) {
        return service.streamStatus(consumerId, streamId);
    }

    @Override
    public SnapshotPage snapshot(String streamId, Long boundaryTso, String pageToken, int limit) {
        return service.snapshot(consumerId, streamId, boundaryTso, pageToken, limit);
    }

    @Override
    public ChangeBatch changes(String streamId, String afterPosition, int limit) {
        return service.changes(consumerId, streamId, afterPosition, limit);
    }

    @Override
    public Map<String, Object> keyLookup(String streamId, String bizKey, Long asOfTso) {
        Map<String, Object> key = new java.util.LinkedHashMap<>();
        key.put("id", bizKey);
        return service.keyLookup(consumerId, streamId, key, asOfTso);
    }

    @Override
    public Map<String, Object> positionAt(String streamId, long timeMs) {
        return service.positionAt(consumerId, streamId, timeMs);
    }

    @Override
    public Map<String, Object> commit(String streamId, String position) {
        return service.commit(consumerId, streamId, position);
    }
}

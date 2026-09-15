package com.dcp.module.outlet.v2;

import com.dcp.module.outlet.OutletPermissionEngine;
import com.dcp.module.outlet.model.ConsumerAccount;
import com.dcp.module.outlet.model.Contract;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * v2 契约桥接:**复用出口权限引擎**(不另写一套裁剪逻辑),把引擎算出的投影计划
 * 变成「契约视图」下发给计算面,或就地投影(平台进程内)。
 *
 * <p>铁律不变:实际授权 = **契约 ∩ 主体**;越权列**连 key 都不出现**;行条件不满足 → 等价删除。
 */
public class PlatformContractViewFactory {

    private final OutletPermissionEngine engine;

    public PlatformContractViewFactory(OutletPermissionEngine engine) {
        this.engine = engine;
    }

    /** 契约视图(下发/缓存用):allowedColumns 已是"契约 ∩ 主体"的结果 */
    public Map<String, Object> viewOf(ConsumerAccount consumer, Contract contract, String streamId,
                                      Collection<String> availableColumns) {
        OutletPermissionEngine.ProjectionPlan plan = engine.plan(consumer, contract, streamId, availableColumns);
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("contractId", contract.getContractId());
        view.put("consumerId", consumer == null ? null : consumer.getConsumerId());
        view.put("stream", streamId);
        view.put("allowedColumns", new ArrayList<>(plan.allowedColumns));
        view.put("maskRules", new LinkedHashMap<>(plan.maskRules));
        view.put("rowCondition", contract.getRowCondition());
        view.put("maxLevel", contract.getMaxLevel());
        // 审计口径:契约裁掉的列 vs 主体侧收窄掉的列(便于解释"为什么这列看不到")
        view.put("trimmedColumns", new ArrayList<>(plan.trimmedColumns));
        view.put("subjectTrimmed", new ArrayList<>(plan.subjectTrimmed));
        return view;
    }

    /** 就地投影一行(平台自身消费路径 / 单测用;与计算面共享同一套可见性语义) */
    public Map<String, Object> projectRow(ConsumerAccount consumer, Contract contract, String streamId,
                                          Collection<String> availableColumns, Map<String, Object> row) {
        return engine.projectRow(engine.plan(consumer, contract, streamId, availableColumns), row);
    }

    /** 拿投影计划本身(需要 rowFilter 时) */
    public OutletPermissionEngine.ProjectionPlan planOf(ConsumerAccount consumer, Contract contract,
                                                        String streamId, Collection<String> availableColumns) {
        return engine.plan(consumer, contract, streamId, availableColumns);
    }

    public OutletPermissionEngine engine() { return engine; }
}

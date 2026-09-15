package com.dcp.outlet.integration;

import com.dcp.outlet.control.DispatchRequest;
import com.dcp.outlet.control.FailureSpec;
import com.dcp.outlet.control.UnitDispatcher;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.worker.CreateUnitCmd;
import com.dcp.outlet.worker.FailurePolicy;
import com.dcp.outlet.worker.UnitRunner;

/**
 * 接线层:把控制面的 {@link DispatchRequest} 映射成计算面的 {@code CreateUnitCmd} 并启动单元。
 *
 * <p>两个模块**互相不知道对方类型**(各自只依赖 SDK),映射只在这一层发生 ——
 * 换成"HTTP 下发到远程 worker"时,只需替换本类的实现,两侧代码不动。
 */
public class UnitDispatcherAdapter implements UnitDispatcher {

    private final UnitRunner runner;

    public UnitDispatcherAdapter(UnitRunner runner) { this.runner = runner; }

    @Override
    public void dispatch(DispatchRequest r) {
        runner.start(new CreateUnitCmd()
                .reqId(r.reqId())
                .unitId(r.unitId())
                .taskId(r.taskId())
                .topic(r.topic())
                .stream(r.stream())
                .partitions(r.partitions())
                .startOffsets(r.startOffsets())
                .consumerGroup(r.consumerGroup())
                .logic(LogicRef.of(r.logicId(), r.logicVersion()))
                .params(r.params())
                .failure(toFailurePolicy(r.failure()))
                .leaseTtlSeconds((int) (r.leaseTtlMillis() / 1000)));
    }

    static FailurePolicy toFailurePolicy(FailureSpec spec) {
        FailurePolicy.OnFailure on = "continue".equalsIgnoreCase(spec.onFailure())
                ? FailurePolicy.OnFailure.SKIP
                : FailurePolicy.OnFailure.STOP;
        return new FailurePolicy(spec.maxRetries(), spec.backoffMillis(), on);
    }
}

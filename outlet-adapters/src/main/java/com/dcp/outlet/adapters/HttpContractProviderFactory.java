package com.dcp.outlet.adapters;

import com.dcp.outlet.worker.ContractProvider;
import com.dcp.outlet.worker.ContractProviderFactory;
import com.dcp.outlet.worker.CreateUnitCmd;

import java.time.Duration;
import java.util.logging.Logger;

/**
 * 生产契约提供者工厂:按单元的 {@code contractRef} 从控制面拉契约视图。
 *
 * <pre>
 *   单元(带 contractRef) → HttpContractProvider → GET 控制面 /api/outlet/contracts/{ref}?stream=
 *      ├─ 命中缓存(未到刷新周期) → 用缓存
 *      ├─ 刷新失败但已有缓存   → 沿用缓存(**不放大权限**,最多延迟一个刷新周期)
 *      └─ 从未拉到 / 单元没带 ref → **拒绝消费(fail-closed)**
 * </pre>
 *
 * <p>刷新周期默认 **30s**:权限收窄/吊销在下一批生效(无需重启单元)。可配置以便压测与自检。
 *
 * <p>注意:{@code consumerId} 只做审计标识传给控制面,**不参与授权判定** ——
 * 视图里的可见列/行条件由「契约 ∩ 主体」在控制面算好(调用方自报身份不能放大权限)。
 */
public class HttpContractProviderFactory implements ContractProviderFactory {

    private static final Logger LOG = Logger.getLogger("outlet-contract");

    private final String controlBaseUrl;
    private final Duration timeout;
    private final long refreshMillis;

    public HttpContractProviderFactory(String controlBaseUrl, Duration timeout, long refreshMillis) {
        this.controlBaseUrl = controlBaseUrl;
        this.timeout = timeout == null ? Duration.ofSeconds(3) : timeout;
        this.refreshMillis = refreshMillis <= 0 ? 30_000L : refreshMillis;
    }

    public HttpContractProviderFactory(String controlBaseUrl) {
        this(controlBaseUrl, Duration.ofSeconds(3), 30_000L);
    }

    public long refreshMillis() { return refreshMillis; }

    @Override
    public ContractProvider forUnit(CreateUnitCmd cmd) {
        String ref = cmd.contractRef();
        if (ref == null || ref.isBlank()) {
            // 没带契约 = 无法判定可见性 → 拒绝(fail-closed)。绝不退回"全放行"。
            String unitId = cmd.unitId();
            return () -> {
                throw new IllegalStateException(
                        "单元 " + unitId + " 未携带 contractRef —— 拒绝消费(fail-closed)");
            };
        }
        LOG.info(() -> "unit " + cmd.unitId() + " 契约视图来源:" + ref
                + "(刷新周期 " + refreshMillis + "ms)");
        return new HttpContractProvider(controlBaseUrl, ref, cmd.consumerGroup(), timeout, refreshMillis);
    }
}

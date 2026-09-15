package com.dcp.outlet.adapters;

import com.dcp.outlet.control.LeaseManager;
import com.dcp.outlet.worker.LeaseGuard;

import java.util.function.LongSupplier;

/**
 * 把计算面的 {@link LeaseGuard} 接到控制面的 {@link LeaseManager}:
 * 续约失败(= 租约已被他人接管或释放)→ 单元自停,防双消费。
 */
public class LeaseBackedLeaseGuard implements LeaseGuard {

    private final LeaseManager leases;
    private final long ttlMillis;
    private final LongSupplier clock;

    public LeaseBackedLeaseGuard(LeaseManager leases, long ttlMillis, LongSupplier clock) {
        this.leases = leases; this.ttlMillis = ttlMillis; this.clock = clock;
    }

    @Override
    public boolean renew(String unitId) {
        return leases.renew(unitId, ttlMillis, clock.getAsLong());
    }
}

package com.dcp.outlet.worker.support;

import com.dcp.outlet.worker.LeaseGuard;

import java.util.concurrent.atomic.AtomicInteger;

/** 可脚本化租约:前 N 次续约成功,之后失败(用于验证"续约失败即自停")。 */
public class StubLease implements LeaseGuard {

    private final int successTimes;
    private final AtomicInteger calls = new AtomicInteger();

    public StubLease(int successTimes) { this.successTimes = successTimes; }

    public int calls() { return calls.get(); }

    @Override
    public boolean renew(String unitId) { return calls.incrementAndGet() <= successTimes; }
}

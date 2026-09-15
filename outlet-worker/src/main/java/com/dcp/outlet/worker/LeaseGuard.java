package com.dcp.outlet.worker;

/**
 * 租约守卫。单元每轮循环续约;**连续续约失败即自停**(防双消费)。
 */
public interface LeaseGuard {

    /** @return true = 续约成功(或仍有效);false = 已失效,单元必须停止消费 */
    boolean renew();
}

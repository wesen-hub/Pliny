package com.dcp.outlet.rt;

/**
 * 契约提供者:平台**定期刷新**,收窄/吊销在**下一批生效**(不需要重启单元)。
 */
public interface ContractProvider {

    ContractView current();
}

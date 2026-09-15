package com.dcp.outlet.control;

import java.util.Optional;

/** 契约查询端口(复用出口模块的 contract 表)。 */
public interface ContractLookup {

    Optional<ContractInfo> find(String contractRef);
}

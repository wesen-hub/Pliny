package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.ChangeRecord;

import java.util.List;

/** 契约裁剪端口:把解析后的事件收敛成"逻辑被允许看到"的记录。 */
public interface ContractFilter {

    List<ChangeRecord> apply(ContractView contract, String stream, List<DecodedMessage> events);
}

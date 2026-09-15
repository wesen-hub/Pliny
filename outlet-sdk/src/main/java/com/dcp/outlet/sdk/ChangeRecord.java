package com.dcp.outlet.sdk;

import java.util.Map;

/** 单条变更记录(批 / 流统一结构,D17)。 */
public interface ChangeRecord {

    Op op();

    /** 主键(列 → 值)。 */
    Map<String, Object> key();

    /** UPDATE / DELETE 的前值;INSERT / SNAPSHOT 可能为空。 */
    Map<String, Object> before();

    /** 变更后整行 / 快照行;DELETE 可能为空。 */
    Map<String, Object> after();

    /** 事件提交时间戳(TiDB TSO)。 */
    long tso();

    int partition();

    /** 在分区内的 offset(排障 / 死信定位用)。 */
    long offset();
}

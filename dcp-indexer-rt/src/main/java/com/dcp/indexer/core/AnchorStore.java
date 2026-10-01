package com.dcp.indexer.core;

import java.util.List;

/**
 * 锚点存储端口(生产实现 = 元数据库 {@code chg_anchor} 表;测试实现 = 内存)。
 *
 * <p>关键索引:{@code (topic, partition_, epoch, fence, commit_ts)} —— 支撑"二分找 ≤ TSO 的最大栅栏点"。
 */
public interface AnchorStore {

    void save(Anchor anchor);

    /**
     * 栅栏查询(起点计算唯一路径, 栅栏方案 §3.2):
     * 返回 {@code fence=1 且 commitTs ≤ tso} 的最大栅栏点(同分区同代);无则 {@code null}。
     *
     * <p>起点语义:从 F.offset 起消费, 由出口持续过滤(§3.3)产出 >T0 事件;从不失败。
     */
    Anchor lastFenceAtOrBefore(String topic, int partition, int epoch, long tso);

    /**
     * 全量锚点查询(排障/自检观测用, 不参与起点计算)。
     * 返回 {@code commitTs ≤ tso} 的最大锚点(同分区同代);无则 {@code null}。
     */
    Anchor lastAtOrBefore(String topic, int partition, int epoch, long tso);

    /** 删除 {@code wallTime < before} 的锚点(保留期清理)。返回删除条数。 */
    int prune(long before);

    /** 某分区某代的锚点(按 offset 升序),排障 / 自检用。 */
    List<Anchor> list(String topic, int partition, int epoch);
}

package com.dcp.outlet.control;

import java.util.List;

/**
 * 锚点存储端口(生产实现 = 元数据库 {@code chg_anchor} 表;测试实现 = 内存)。
 *
 * <p>关键索引:{@code (topic, partition_, epoch, commit_ts, offset_)} —— 支撑"二分找 ≤ TSO 的最大锚点"。
 */
public interface AnchorStore {

    void save(Anchor anchor);

    /**
     * 二分查询:返回 {@code commitTs ≤ tso} 的最大锚点(同分区同代);无则 {@code null}。
     *
     * <p>这是起点计算的第一步 —— 把"从头扫"变成"从最近的锚点短扫"。
     */
    Anchor lastAtOrBefore(String topic, int partition, int epoch, long tso);

    /** 删除 {@code wallTime < before} 的锚点(保留期清理)。返回删除条数。 */
    int prune(long before);

    /** 某分区某代的锚点(按 offset 升序),排障 / 自检用。 */
    List<Anchor> list(String topic, int partition, int epoch);
}

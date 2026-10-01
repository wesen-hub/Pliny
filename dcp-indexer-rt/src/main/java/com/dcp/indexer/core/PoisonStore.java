package com.dcp.indexer.core;

import java.util.List;

/**
 * 索引器分区污染死信存储(C1):解析失败后该分区不再产栅栏, 查询退化 fallback(宁慢勿错)。
 *
 * <p>持久化语义:污染跨进程重启成立, 直到人工介入确认后删除记录恢复采样
 * (生产实现 = 元数据库 {@code chg_anchor_poison} 表;测试实现 = 内存)。
 */
public interface PoisonStore {

    /** 记录污染(幂等, 重复 mark 不产生副作用)。 */
    void mark(String topic, int partition, int epoch);

    /** 该分区该代是否已污染。 */
    boolean poisoned(String topic, int partition, int epoch);

    /** 全部污染记录(启动恢复用)。 */
    List<Poison> list();
}

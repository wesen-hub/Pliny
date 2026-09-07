package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 全局 seq 发号器(V3 §3.2):区间预分配, 数据库行锁保证并发不重。
 *
 * <pre>
 *   UPDATE seq_counter SET cur = cur + batch WHERE table_name = ?   -- 领到 [start, start+batch)
 *   每条数据依次用 start, start+1 ...(多任务/多并发 worker 共享同一逻辑表计数器)
 * </pre>
 *
 * 特性:
 * - 并发安全: UPDATE 行锁, 区间互不重叠;
 * - 允许间隙: 写者崩溃丢号段 → 只要求"后发号 > 先发号", 间隙无害;
 * - 粒度: 每逻辑表一个计数器(可比性边界 = 会竞争同一行的范围)。
 */
@Slf4j
@Component
public class SeqAllocator {

    private final JdbcTemplate metaJdbc;

    public SeqAllocator(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    /**
     * 领号段 [start, end] 闭区间(batch 个号)。
     *
     * @param logicalTable 逻辑表名(seq_counter 主键)
     * @param batch        批量大小(如 1000, 摊销 DB 开销)
     */
    public long[] allocate(String logicalTable, int batch) {
        int rows = metaJdbc.update(
                "UPDATE seq_counter SET cur = cur + ? WHERE table_name = ?", batch, logicalTable);
        if (rows == 0) {
            // 首见: 插入初始区间 [1, batch]
            metaJdbc.update(
                    "INSERT INTO seq_counter (table_name, cur) VALUES (?, ?)", logicalTable, batch);
        }
        Long cur = metaJdbc.queryForObject(
                "SELECT cur FROM seq_counter WHERE table_name = ?", Long.class, logicalTable);
        long end = cur == null ? batch : cur;
        long start = end - batch + 1;
        return new long[]{start, end};
    }

    /**
     * 元库恢复纪律: seq_counter 必须只增不减地恢复(推进到 max+1000), 否则历史号段可能重用
     * → 不同数据拿到相同 seq → 目标侧收敛选错"最新"。
     */
    public void recoverMonotonic(String logicalTable, long minNext) {
        metaJdbc.update(
                "UPDATE seq_counter SET cur = GREATEST(cur, ?) WHERE table_name = ?", minNext, logicalTable);
    }
}

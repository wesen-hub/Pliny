package com.dcp.module.outlet.sdk;

import java.util.Map;

/**
 * 幂等写入(设计 §6.6 增量段 / §6.4.1 衔接契约 4)。
 *
 * <p>实现必须满足:
 * <ul>
 *   <li>按主键 upsert(重复投递是常态:at-least-once);</li>
 *   <li>**版本收敛**:只允许更高 commitTs 覆盖(否则批↔流交替写入时"旧值覆盖新值");</li>
 *   <li>删除事件按主键删(墓碑)。</li>
 * </ul>
 */
public interface IdempotentWriter {

    /** 写入/更新一行,携带事件 TSO 作为版本 */
    void upsert(String table, Map<String, Object> row, long commitTs);

    /** 删除一行(墓碑) */
    void delete(String table, Map<String, Object> key, long commitTs);
}

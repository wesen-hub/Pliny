package com.dcp.outlet.control;

import com.dcp.outlet.sdk.ChangeRecord;

import java.util.List;

/**
 * 批阶段的**快照源端口**:在 T0 视图上读一个切片(不碰 Kafka)。
 *
 * <p>生产实现 = 平台既有快照读路径(`AS OF T0`;或经出口快照 API);
 * 测试实现 = 内存表。**读的是"某一时刻的状态"**,不是"一段时间的变更"。
 */
public interface SnapshotSource {

    /** 估算行数(用于切片决策);未知返回 -1。 */
    long estimateRows(String stream, long tso);

    /** 读一个切片;返回空列表表示该切片无数据。 */
    List<ChangeRecord> readSlice(String stream, long tso, Slice slice);
}

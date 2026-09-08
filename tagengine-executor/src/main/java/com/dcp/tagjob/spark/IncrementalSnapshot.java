package com.dcp.tagjob.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 增量轮快照(driver 侧; 设计文档 §14 增量打标)。
 *
 * <p>增量模式: 读湖表仅取 _seq>cursor 的变更行(湖表每实体一行当前态 + lakesync 行级 seq),
 * 只对变更实体重算, 写 tag_fact 时删除"本任务这些实体"的旧行再 append —— 不再整 task_id 分区重写。
 * 墓碑行(_is_deleted=1)实体 = 注销: 只清 tag_fact 旧行, 不参与求值。
 * 游标推进 = 本轮变更行 max(_seq); 失败不推进, 重跑幂等(§14.2)。
 *
 * <p>湖表每实体一行 → 变更行维度 = 实体维度, 一次 collect 即可同时得到
 * 变更实体集 / 墓碑实体集 / max seq, 避免多次 action。
 */
public final class IncrementalSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 增量是否启用(游标 >= 0) */
    public boolean active;
    /** 实际使用的增量列名(湖表 _seq 列; 未探测到 = 无法增量, assemble 阶段抛错) */
    public String seqColumn;
    /** 湖表是否存在墓碑列 */
    public boolean hasTombstone;
    /** 本轮变更实体(entityKey 值; 含墓碑实体 —— 写层按此集合删旧行) */
    public List<String> changedEntities = new ArrayList<>();
    /** 其中墓碑实体(注销; 无新标签, 仅清旧行) */
    public List<String> tombstoneEntities = new ArrayList<>();
    /** 本轮变更行 max(_seq)(游标推进值; 无变更行 = -1 不推进) */
    public long maxSeq = -1;

    public boolean hasChanged() {
        return !changedEntities.isEmpty();
    }
}

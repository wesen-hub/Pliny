package com.dcp.tagjob.run;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * 行级 TagSink: 记标签输出 + 每资产命中数(供 run 摘要/命中矩阵)。
 * 每行一个实例, fire 前经 TagActions.bindSink 绑定, 行处理完弃用(分区内不复用, 避免串行状态)。
 * 语义(设计文档 §3/§11①): 命中 = 该资产至少执行一条动作(tag 或 stop/halt 动作); 决策表/平铺只数动作。
 */
public final class RowSink {

    /** 输出的一枚标签(写 tag_fact 的行) */
    public static final class TagEntry {
        public final String tag;
        public final String value;

        TagEntry(String tag, String value) {
            this.tag = tag;
            this.value = value;
        }
    }

    /** 当前行实体值(写 tag_fact 的 entity 列; null → 该行标签不落库只计数) */
    public final String entity;
    private final int assetCount;
    private final List<TagEntry> tags = new ArrayList<>(4);
    /** 每资产(下标)动作次数 = 标签动作 + stop/halt 动作 */
    private final long[] assetActions;
    /** 每资产(下标)标签行数 */
    private final long[] assetTags;
    /** 本行哪些资产至少执行一条动作(资产命中) */
    private final BitSet rowHit;
    /** 当前 fire 的资产下标(动作归属) */
    private int focused = -1;
    private boolean haltRequested = false;
    private long totalActions = 0;

    public RowSink(String entity, int assetCount) {
        this.entity = entity;
        this.assetCount = assetCount;
        this.assetActions = new long[assetCount];
        this.assetTags = new long[assetCount];
        this.rowHit = new BitSet(assetCount);
    }

    public void setFocused(int assetIdx) {
        this.focused = assetIdx;
    }

    /** TagActions.tag 入口 */
    void tag(String path, String value) {
        tags.add(new TagEntry(path, value));
        if (focused >= 0) {
            assetTags[focused]++;
            assetActions[focused]++;
            rowHit.set(focused);
            totalActions++;
        }
    }

    /** TagActions.halt 入口: 停止动作也算一次动作(该资产命中) */
    void requestHalt() {
        haltRequested = true;
        if (focused >= 0) {
            assetActions[focused]++;
            rowHit.set(focused);
            totalActions++;
        }
    }

    /** 取走 halt 标志(每次 fire 后消费; 平铺阶段据此跳过剩余平铺规则, 规则流不受影响) */
    public boolean consumeHalt() {
        boolean h = haltRequested;
        haltRequested = false;
        return h;
    }

    /** 直接标记资产本行命中(用于规则流: 流内任一动作 → 流命中, §3 被引流命中=链内至少一条动作) */
    public void markHit(int assetIdx) {
        if (assetIdx >= 0 && assetIdx < assetCount) {
            rowHit.set(assetIdx);
        }
    }

    public boolean isHaltRequested() {
        return haltRequested;
    }

    public long totalActions() {
        return totalActions;
    }

    public List<TagEntry> tags() {
        return tags;
    }

    public BitSet rowHit() {
        return rowHit;
    }

    /** 资产本行命中的标签行数 */
    public long tagsOf(int assetIdx) {
        return assetTags[assetIdx];
    }
}

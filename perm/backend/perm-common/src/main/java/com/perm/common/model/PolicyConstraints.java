package com.perm.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 策略约束（策略三件套中的约束部分）
 * visible / rowExpr / columns / excludeColumns / masks / tagScope
 */
public class PolicyConstraints {

    private final Visibility visible;
    private final PredicateNode rowExpr;
    private final List<String> columns;
    private final List<String> excludeColumns;
    private final List<MaskSpec> masks;
    private final TagScope tagScope;

    private PolicyConstraints(Builder b) {
        this.visible = b.visible == null ? Visibility.CONDITIONAL : b.visible;
        this.rowExpr = b.rowExpr;
        this.columns = b.columns == null ? null : List.copyOf(b.columns);
        this.excludeColumns = List.copyOf(b.excludeColumns);
        this.masks = List.copyOf(b.masks);
        this.tagScope = b.tagScope;
    }

    public Visibility visible() {
        return visible;
    }

    public PredicateNode rowExpr() {
        return rowExpr;
    }

    /** 可见列；null 表示无列约束（不裁剪） */
    public List<String> columns() {
        return columns;
    }

    public List<String> excludeColumns() {
        return excludeColumns;
    }

    public List<MaskSpec> masks() {
        return masks;
    }

    public TagScope tagScope() {
        return tagScope;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Visibility visible;
        private PredicateNode rowExpr;
        private List<String> columns;
        private final List<String> excludeColumns = new ArrayList<>();
        private final List<MaskSpec> masks = new ArrayList<>();
        private TagScope tagScope;

        public Builder visible(Visibility v) {
            this.visible = v;
            return this;
        }

        public Builder rowExpr(PredicateNode v) {
            this.rowExpr = v;
            return this;
        }

        public Builder columns(List<String> v) {
            this.columns = v;
            return this;
        }

        public Builder excludeColumn(String v) {
            this.excludeColumns.add(v);
            return this;
        }

        public Builder mask(String column, MaskType type) {
            this.masks.add(new MaskSpec(column, type));
            return this;
        }

        public Builder tagScope(TagScope v) {
            this.tagScope = v;
            return this;
        }

        public PolicyConstraints build() {
            return new PolicyConstraints(this);
        }
    }

    /** 掩码规格：列 × 掩码类型 */
    public record MaskSpec(String column, MaskType type) {
    }

    /**
     * 标签范围（R-T7 tagScope 产物字段）
     */
    public record TagScope(
            String tagPath,
            TagExpandMode expandMode,
            String snapshotVersion,
            String targetKey,
            /** tag.claim 所需门槛（R-T2） */
            int requiredLevel,
            /** 模式 A 实体列表（标签服务 getEntities 返回，跨库"值进产物"唯一例外） */
            List<String> entities) {

        public TagScope {
            entities = entities == null ? List.of() : List.copyOf(entities);
        }
    }
}

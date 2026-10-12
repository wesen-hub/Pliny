package com.perm.common.merge;

import com.perm.common.model.Policy;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.PredicateNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 约束合并（R-D 系列冲突消解规则）
 *   R-D7  行谓词：AND 连接
 *   R-D8  列可见性：交集 − excludeColumns
 *   R-D9  掩码：同列冲突取最严（FULL > NULL > HASH > PARTIAL > REPLACE）
 *   R-D16 义务：并集，同义务取更强（sync > async）
 */
public final class ConstraintMerger {

    private ConstraintMerger() {
    }

    /** 合并行谓词：多策略 AND 连接（R-D7）；全空 → null（不注入） */
    public static PredicateNode mergeRowPredicates(List<PredicateNode> predicates) {
        List<PredicateNode> list = predicates.stream().filter(p -> p != null).toList();
        if (list.isEmpty()) {
            return null;
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        return new com.perm.common.model.AndNode(list);
    }

    /** 合并列可见性：交集 − exclude（R-D8）；无任何列约束 → null（不裁剪） */
    public static List<String> mergeColumns(List<List<String>> columnsList, List<List<String>> excludeList) {
        List<String> first = null;
        boolean constrained = false;
        for (List<String> cols : columnsList) {
            if (cols == null) {
                continue;
            }
            constrained = true;
            if (first == null) {
                first = new ArrayList<>(cols);
            } else {
                first.retainAll(cols);
            }
        }
        for (List<String> ex : excludeList) {
            if (ex == null || ex.isEmpty()) {
                continue;
            }
            constrained = true;
        }
        if (!constrained) {
            return null;
        }
        if (first == null) {
            first = new ArrayList<>();
        }
        for (List<String> ex : excludeList) {
            if (ex != null) {
                first.removeAll(ex);
            }
        }
        return first;
    }

    /** 合并掩码：同列冲突取最严（R-D9） */
    public static List<PolicyConstraints.MaskSpec> mergeMasks(List<List<PolicyConstraints.MaskSpec>> maskLists) {
        Map<String, PolicyConstraints.MaskSpec> map = new LinkedHashMap<>();
        for (List<PolicyConstraints.MaskSpec> masks : maskLists) {
            if (masks == null) {
                continue;
            }
            for (PolicyConstraints.MaskSpec m : masks) {
                PolicyConstraints.MaskSpec existing = map.get(m.column());
                if (existing == null || m.type().severity() > existing.type().severity()) {
                    map.put(m.column(), m);
                }
            }
        }
        return new ArrayList<>(map.values());
    }

    /** 合并义务：并集，同义务取更强（R-D16） */
    public static List<Policy.Obligation> mergeObligations(List<List<Policy.Obligation>> obligationLists) {
        Map<String, Policy.Obligation> map = new LinkedHashMap<>();
        for (List<Policy.Obligation> list : obligationLists) {
            if (list == null) {
                continue;
            }
            for (Policy.Obligation o : list) {
                Policy.Obligation existing = map.get(o.type());
                if (existing == null || AUDIT_STRENGTH(o.mode()) > AUDIT_STRENGTH(existing.mode())) {
                    map.put(o.type(), o);
                }
            }
        }
        return new ArrayList<>(map.values());
    }

    private static int AUDIT_STRENGTH(String mode) {
        return "sync".equals(mode) ? 2 : 1;
    }

    /** 合并 tagScope：多条 GRANT 各自带标签约束时取门槛最高（最严） */
    public static PolicyConstraints.TagScope mergeTagScope(List<PolicyConstraints.TagScope> scopes) {
        List<PolicyConstraints.TagScope> list = scopes.stream().filter(s -> s != null).toList();
        if (list.isEmpty()) {
            return null;
        }
        PolicyConstraints.TagScope base = list.get(0);
        int maxLevel = base.requiredLevel();
        for (PolicyConstraints.TagScope s : list) {
            maxLevel = Math.max(maxLevel, s.requiredLevel());
        }
        return new PolicyConstraints.TagScope(
                base.tagPath(), base.expandMode(), base.snapshotVersion(),
                base.targetKey(), maxLevel, base.entities());
    }
}

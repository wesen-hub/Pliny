package com.perm.common.conflict;

import com.perm.common.model.AndNode;
import com.perm.common.model.EqNode;
import com.perm.common.model.InNode;
import com.perm.common.model.OrNode;
import com.perm.common.model.Policy;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.PredicateNode;
import com.perm.common.model.RangeNode;
import com.perm.common.model.RegexNode;
import com.perm.common.model.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 冲突检测引擎（R-M7/R-M8）
 * 九类检测 C1~C9，按可判定性分三级：
 *   ① 精确判定（ERROR 阻断）：C7 标签引用失效 / C8 变量引用失效 /
 *      C2/C3 简单谓词值域交集分析（等值集合交集、范围区间交集、同列多条件自检）
 *   ② 近似判定（WARNING 确认）：C1/C4/C5/C6/C9 集合运算精确可判但意图需人工确认
 *   ③ 不可判定降级（INFO 提示）：含 ref 绑定/OR/REGEX 的复杂谓词
 * 检测时机：策略提交 → 审批之间（R-M3 校验第一步）。
 */
public final class ConflictDetector {

    // ============ 值域模型（谓词值域交集分析的载体） ============

    /** 列值域：值集合 / 区间 / 未知（⊤） / 矛盾（空交集） */
    private sealed interface Domain
            permits ValueSet, Interval, Unknown, DomainContradiction {
    }

    private record ValueSet(Set<Object> values) implements Domain {
    }

    private record Interval(Object min, Object max) implements Domain {
    }

    private record Unknown() implements Domain {
        static final Unknown INSTANCE = new Unknown();
    }

    /** 主体属性目录（C8 校验输入）；null = 跳过 C8 */
    public record AttrRegistry(Set<String> knownAttrs) {
        public static AttrRegistry of(String... attrs) {
            return new AttrRegistry(Set.of(attrs));
        }

        public boolean known(String ref) {
            return knownAttrs.contains(ref);
        }
    }

    /** 标签目录（C7 校验输入）；null = 跳过 C7 */
    public record TagRegistry(Set<String> knownTagPaths) {
        public static TagRegistry of(String... tagPaths) {
            return new TagRegistry(Set.of(tagPaths));
        }

        public boolean known(String tagPath) {
            return knownTagPaths.contains(tagPath);
        }
    }

    private ConflictDetector() {
    }

    /**
     * 冲突检测入口
     * @param newPolicy       新策略（提交审批前）
     * @param activePolicies  同资源域 active 策略（策略间检测对象）
     * @param attrRegistry    已知主体属性目录（C8）；null 跳过
     * @param tagRegistry     已知标签目录（C7）；null 跳过
     */
    public static ConflictReport detect(Policy newPolicy, List<Policy> activePolicies,
                                        AttrRegistry attrRegistry, TagRegistry tagRegistry) {
        List<ConflictItem> items = new ArrayList<>();

        // ① 策略内自检（R-M8-①）
        detectInternal(newPolicy, attrRegistry, tagRegistry, items);

        // ② 策略间检测（R-M8-②③）：仅同域（subject 相交 × 同 resource × 同 action 相交）
        for (Policy other : activePolicies) {
            if (other.policyId().equals(newPolicy.policyId())) {
                continue;
            }
            if (!sameDomain(newPolicy, other)) {
                continue;
            }
            detectBetween(newPolicy, other, items);
        }

        return new ConflictReport(newPolicy.policyId(), items);
    }

    // ============ 策略内自检 ============

    private static void detectInternal(Policy p, AttrRegistry attrs, TagRegistry tags,
                                       List<ConflictItem> items) {
        PolicyConstraints c = p.constraints();

        // C8 变量引用失效：rowExpr 全部 ref 均可解析
        if (attrs != null && c.rowExpr() != null) {
            for (String ref : collectRefs(c.rowExpr())) {
                if (!attrs.known(ref)) {
                    items.add(new ConflictItem(ConflictSeverity.ERROR,
                            ConflictType.VAR_REF_INVALID, p.policyId(),
                            "rowExpr 引用 ${" + ref + "} 无法从已知主体属性目录解析",
                            subjectDesc(p), "修正引用或补充属性渠道（三渠道扩展）"));
                }
            }
        }

        // C7 标签引用失效：tagScope 的 tagPath 在标签目录中存在
        if (tags != null && c.tagScope() != null) {
            String tagPath = c.tagScope().tagPath();
            if (tagPath != null && !tags.known(tagPath)) {
                items.add(new ConflictItem(ConflictSeverity.ERROR,
                        ConflictType.TAG_REF_INVALID, p.policyId(),
                        "TAG_REF 引用不存在的标签路径: " + tagPath,
                        subjectDesc(p), "检查标签服务目录或修正 tagPath"));
            }
        }

        // C2 策略内矛盾：AND 子节点同列多条件值域交集为空（死策略）
        if (c.rowExpr() != null) {
            Map<String, List<Domain>> domains = extractDomains(c.rowExpr());
            for (Map.Entry<String, List<Domain>> e : domains.entrySet()) {
                Domain merged = intersectAll(e.getValue());
                if (merged == DomainContradiction.INSTANCE) {
                    items.add(new ConflictItem(ConflictSeverity.ERROR,
                            ConflictType.ROW_PREDICATE_CONTRADICTION, p.policyId(),
                            "同列 " + e.getKey() + " 多条件值域交集为空——该策略永不命中（死策略）",
                            subjectDesc(p), "修正矛盾条件或拆分策略"));
                }
            }
        }
    }

    // ============ 策略间检测 ============

    private static void detectBetween(Policy np, Policy op, List<ConflictItem> items) {
        // C1 Effect 对撞：同域 GRANT 与 DENY 并存（DENY-overrides 裁决，但需确认）
        if (np.effect() != op.effect()) {
            items.add(new ConflictItem(ConflictSeverity.WARNING,
                    ConflictType.EFFECT_COLLISION, op.policyId(),
                    "同域内 " + np.effect() + "(" + np.policyId() + ") 与 "
                            + op.effect() + "(" + op.policyId() + ") 并存，判定按 DENY-overrides（R-D2）",
                    subjectDesc(np) + " ∩ " + subjectDesc(op),
                    "确认对撞是否符合预期（如黑名单场景）"));
        }

        PolicyConstraints nc = np.constraints();
        PolicyConstraints oc = op.constraints();

        // C2/C3 行谓词值域分析
        if (nc.rowExpr() != null && oc.rowExpr() != null) {
            Map<String, List<Domain>> nd = extractDomains(nc.rowExpr());
            Map<String, List<Domain>> od = extractDomains(oc.rowExpr());
            for (Map.Entry<String, List<Domain>> e : nd.entrySet()) {
                List<Domain> otherDomains = od.get(e.getKey());
                if (otherDomains == null) {
                    continue;
                }
                Domain a = intersectAll(e.getValue());
                Domain b = intersectAll(otherDomains);
                Domain inter = intersect(a, b);
                if (inter == DomainContradiction.INSTANCE) {
                    items.add(new ConflictItem(ConflictSeverity.ERROR,
                            ConflictType.ROW_PREDICATE_CONTRADICTION, op.policyId(),
                            "列 " + e.getKey() + " 值域交集为空：" + describe(a) + " ∩ " + describe(b),
                            subjectDesc(np) + " ∩ " + subjectDesc(op),
                            "同一主体域下 " + np.policyId() + " 永不命中，修正谓词"));
                } else if (subsetOf(a, b)) {
                    items.add(new ConflictItem(ConflictSeverity.WARNING,
                            ConflictType.ROW_PREDICATE_REDUNDANT, op.policyId(),
                            "列 " + e.getKey() + " 值域被完全覆盖：" + describe(a) + " ⊆ " + describe(b),
                            subjectDesc(np) + " ∩ " + subjectDesc(op),
                            "新策略的该列约束冗余（被 " + op.policyId() + " 覆盖），确认意图"));
                } else if (inter instanceof Unknown) {
                    items.add(new ConflictItem(ConflictSeverity.INFO,
                            ConflictType.ROW_PREDICATE_REDUNDANT, op.policyId(),
                            "列 " + e.getKey() + " 含 ref 绑定/OR/REGEX 复杂谓词，值域不可静态判定",
                            subjectDesc(np) + " ∩ " + subjectDesc(op),
                            "人工核对谓词对"));
                }
            }
        }

        // C4 列可见性冲突：同列在 nc.columns 且 oc.excludeColumns
        if (nc.columns() != null && oc.excludeColumns() != null) {
            List<String> conflict = nc.columns().stream()
                    .filter(oc.excludeColumns()::contains)
                    .toList();
            if (!conflict.isEmpty()) {
                items.add(new ConflictItem(ConflictSeverity.WARNING,
                        ConflictType.COLUMN_VISIBILITY_CONFLICT, op.policyId(),
                        "列 " + conflict + " 在新策略可见但被 " + op.policyId()
                                + " exclude——合并后不可见（R-D8 交集 − exclude）",
                        subjectDesc(np) + " ∩ " + subjectDesc(op),
                        "确认列覆盖是否符合预期"));
            }
        }

        // C5 掩码冲突：同列不同掩码类型
        for (PolicyConstraints.MaskSpec nm : nc.masks()) {
            for (PolicyConstraints.MaskSpec om : oc.masks()) {
                if (nm.column().equals(om.column()) && nm.type() != om.type()) {
                    items.add(new ConflictItem(ConflictSeverity.WARNING,
                            ConflictType.MASK_CONFLICT, op.policyId(),
                            "列 " + nm.column() + " 掩码冲突：" + nm.type() + " vs " + om.type()
                                    + "——合并取最严 " + stricter(nm.type(), om.type()) + "（R-D9）",
                            subjectDesc(np) + " ∩ " + subjectDesc(op),
                            "确认较宽松一侧的意图是否被吞没"));
                }
            }
        }

        // C6 义务冲突：同义务不同强度
        for (Policy.Obligation no : np.obligations()) {
            for (Policy.Obligation oo : op.obligations()) {
                if (no.type().equals(oo.type()) && !no.mode().equals(oo.mode())) {
                    items.add(new ConflictItem(ConflictSeverity.WARNING,
                            ConflictType.OBLIGATION_CONFLICT, op.policyId(),
                            "义务 " + no.type() + " 强度冲突：" + no.mode() + " vs " + oo.mode()
                                    + "——合并取更强 " + strongerMode(no.mode(), oo.mode()) + "（R-D16）",
                            subjectDesc(np) + " ∩ " + subjectDesc(op),
                            "确认较弱一侧的意图是否被覆盖"));
                }
            }
        }

        // C9 权限静默收缩：新策略列集合/谓词收窄现有权限（影响面分析）
        if (oc.columns() != null
                && (nc.columns() == null || !new HashSet<>(nc.columns()).containsAll(oc.columns()))) {
            List<String> shrunk = oc.columns() == null ? List.of()
                    : oc.columns().stream()
                    .filter(col -> nc.columns() == null || !nc.columns().contains(col))
                    .toList();
            items.add(new ConflictItem(ConflictSeverity.WARNING,
                    ConflictType.PERMISSION_SHRINK, op.policyId(),
                    "新策略使主体域内现有列权限收窄：" + shrunk,
                    subjectDesc(np) + " ∩ " + subjectDesc(op),
                    "确认收窄是有意为之（否则补充列授权）"));
        }
    }

    // ============ 值域提取与交集分析 ============

    /** 提取行谓词中每列的约束域（仅字面量可静态分析；ref/OR/REGEX → Unknown） */
    private static Map<String, List<Domain>> extractDomains(PredicateNode node) {
        Map<String, List<Domain>> result = new HashMap<>();
        collect(node, result);
        return result;
    }

    private static void collect(PredicateNode node, Map<String, List<Domain>> out) {
        if (node instanceof EqNode e && e.value() instanceof Value.Lit lit) {
            out.computeIfAbsent(e.column(), k -> new ArrayList<>())
                    .add(new ValueSet(Set.of(lit.value())));
        } else if (node instanceof EqNode e && e.value() instanceof Value.Ref) {
            out.computeIfAbsent(e.column(), k -> new ArrayList<>()).add(Unknown.INSTANCE);
        } else if (node instanceof InNode in) {
            out.computeIfAbsent(in.column(), k -> new ArrayList<>()).add(Unknown.INSTANCE);
        } else if (node instanceof RangeNode r) {
            Domain d = (r.min() instanceof Value.Lit || r.max() instanceof Value.Lit)
                    ? new Interval(litOrNull(r.min()), litOrNull(r.max()))
                    : Unknown.INSTANCE;
            out.computeIfAbsent(r.column(), k -> new ArrayList<>()).add(d);
        } else if (node instanceof RegexNode rx) {
            out.computeIfAbsent(rx.column(), k -> new ArrayList<>()).add(Unknown.INSTANCE);
        } else if (node instanceof AndNode a) {
            for (PredicateNode child : a.children()) {
                collect(child, out);
            }
        } else if (node instanceof OrNode) {
            // OR 语义复杂，v1 降级 Unknown（保守不误报）
        }
        // ConstNode 不产生约束
    }

    private static Object litOrNull(Value v) {
        return v instanceof Value.Lit lit ? lit.value() : null;
    }

    /** 多个域合并（AND 语义 = 交集） */
    private static Domain intersectAll(List<Domain> domains) {
        Domain acc = null;
        for (Domain d : domains) {
            acc = acc == null ? d : intersect(acc, d);
            if (acc == DomainContradiction.INSTANCE) {
                return acc;
            }
        }
        return acc;
    }

    /** 两域交集 */
    private static Domain intersect(Domain a, Domain b) {
        if (a instanceof Unknown || b instanceof Unknown) {
            return Unknown.INSTANCE;
        }
        if (a instanceof ValueSet vs1 && b instanceof ValueSet vs2) {
            Set<Object> inter = new HashSet<>(vs1.values());
            inter.retainAll(vs2.values());
            return inter.isEmpty() ? DomainContradiction.INSTANCE : new ValueSet(inter);
        }
        if (a instanceof Interval i1 && b instanceof Interval i2) {
            Object min = maxOf(i1.min(), i2.min());
            Object max = minOf(i1.max(), i2.max());
            if (min != null && max != null && compare(min, max) > 0) {
                return DomainContradiction.INSTANCE;
            }
            return new Interval(min, max);
        }
        return Unknown.INSTANCE;
    }

    /** a 是否被 b 覆盖（子集判定） */
    private static boolean subsetOf(Domain a, Domain b) {
        if (a instanceof Unknown) {
            return false;
        }
        if (a instanceof ValueSet vs && b instanceof ValueSet bs) {
            return bs.values().containsAll(vs.values());
        }
        if (a instanceof Interval i && b instanceof Interval bi) {
            boolean minOk = bi.min() == null || (i.min() != null && compare(i.min(), bi.min()) >= 0);
            boolean maxOk = bi.max() == null || (i.max() != null && compare(i.max(), bi.max()) <= 0);
            return minOk && maxOk;
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(Object a, Object b) {
        if (a instanceof Comparable ca && b instanceof Comparable cb) {
            return ca.compareTo(cb);
        }
        return 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object maxOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable) a).compareTo(b) >= 0 ? a : b;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object minOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable) a).compareTo(b) <= 0 ? a : b;
    }

    private static String describe(Domain d) {
        if (d instanceof ValueSet vs) {
            return "{" + vs.values().stream().map(String::valueOf)
                    .collect(Collectors.joining(", ")) + "}";
        }
        if (d instanceof Interval i) {
            return "[" + (i.min() == null ? "-∞" : i.min()) + ", "
                    + (i.max() == null ? "+∞" : i.max()) + "]";
        }
        return "未知(⊤)";
    }

    /** 空交集标记（矛盾） */
    private static final class DomainContradiction implements Domain {
        static final DomainContradiction INSTANCE = new DomainContradiction();
    }

    // ============ 辅助 ============

    /** 收集行谓词全部 subject ref（C8 校验输入） */
    private static Set<String> collectRefs(PredicateNode node) {
        Set<String> refs = new HashSet<>();
        collectRefs(node, refs);
        return refs;
    }

    private static void collectRefs(PredicateNode node, Set<String> out) {
        if (node instanceof EqNode e) {
            if (e.value() instanceof Value.Ref r) {
                out.add(r.ref());
            }
        } else if (node instanceof InNode in) {
            if (in.value() instanceof Value.Ref r) {
                out.add(r.ref());
            }
        } else if (node instanceof RangeNode r) {
            if (r.min() instanceof Value.Ref rm) {
                out.add(rm.ref());
            }
            if (r.max() instanceof Value.Ref rx) {
                out.add(rx.ref());
            }
        } else if (node instanceof AndNode a) {
            a.children().forEach(c -> collectRefs(c, out));
        } else if (node instanceof OrNode o) {
            o.children().forEach(c -> collectRefs(c, out));
        }
    }

    /** 同域判定：subject 类型相同且 ids 相交 + 同 resourceId + action 相交 */
    private static boolean sameDomain(Policy a, Policy b) {
        if (!a.subjectType().equals(b.subjectType())) {
            return false;
        }
        if (!a.resourceId().equals(b.resourceId())) {
            return false;
        }
        boolean subjectIntersect = a.subjectIds().stream().anyMatch(b.subjectIds()::contains);
        if (!subjectIntersect) {
            return false;
        }
        boolean actionIntersect = a.actions().isEmpty() || b.actions().isEmpty()
                || a.actions().stream().anyMatch(b.actions()::contains);
        return actionIntersect;
    }

    private static String subjectDesc(Policy p) {
        return p.subjectType() + ":" + String.join("/", p.subjectIds());
    }

    private static com.perm.common.model.MaskType stricter(com.perm.common.model.MaskType a,
                                                           com.perm.common.model.MaskType b) {
        return a.severity() >= b.severity() ? a : b;
    }

    private static String strongerMode(String a, String b) {
        return "sync".equals(a) ? a : b;
    }
}

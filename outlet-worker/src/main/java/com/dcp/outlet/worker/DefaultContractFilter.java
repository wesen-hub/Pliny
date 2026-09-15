package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.Mode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 契约裁剪(设计 §7.6):**列白名单 → 行条件 → 脱敏**。
 *
 * <p>发生在 {@code process()} **之前** —— 越权列在到达业务逻辑前就已被丢弃。
 */
public class DefaultContractFilter implements ContractFilter {

    private final RowConditionEvaluator rowEvaluator;
    private final Masker masker;
    /** 可选的固定契约视图(未接权限引擎时使用);为 null 时用调用方传入的契约。 */
    private final ContractView fixedView;

    public DefaultContractFilter(RowConditionEvaluator rowEvaluator, Masker masker, ContractView fixedView) {
        this.rowEvaluator = rowEvaluator;
        this.masker = masker;
        this.fixedView = fixedView;
    }

    public DefaultContractFilter(RowConditionEvaluator rowEvaluator, Masker masker) {
        this(rowEvaluator, masker, null);
    }

    @Override
    public List<ChangeRecord> apply(ContractView contract, String stream, List<DecodedMessage> events) {
        ContractView effective = fixedView != null ? fixedView : contract;
        Set<String> whitelist = effective == null ? null : effective.columnWhitelist(stream);
        Map<String, String> masks = effective == null ? Map.of() : effective.maskRules();
        int maxLevel = effective == null ? -1 : effective.maxLevel();
        String rowCondition = effective == null ? null : effective.rowCondition();

        // ★ 空视图 = 契约无效 / 未覆盖该流 / 主体可见列为空 → **一条都不交付**(fail-closed)。
        //   控制面下发放空视图就是这个意思("调用方 fail-closed");
        //   若当成"裁掉所有列"继续交付,业务逻辑会收到一批空行(key 仍在)—— 等于"吊销了还在喂数据"。
        if (whitelist != null && whitelist.isEmpty()) {
            return List.of();
        }

        List<ChangeRecord> out = new ArrayList<>(events.size());
        for (DecodedMessage e : events) {
            if (e.kind() != DecodedMessage.Kind.EVENT) continue;

            // ★ 行条件必须用**原始行**判定:条件里引用的列(如 status)不一定在列白名单内,
            //    若先裁列再判条件,会因缺列导致条件恒不成立 → 正确行被误丢(与平台引擎语义保持一致)
            Map<String, Object> rawJudge = (e.after() != null && !e.after().isEmpty()) ? e.after() : e.before();
            if (rowCondition != null && !rowCondition.isBlank()
                    && rowEvaluator != null && !rowEvaluator.test(rowCondition, rawJudge)) {
                continue;                                  // 不满足数据范围 → 不进逻辑
            }

            Map<String, Object> after = project(e.after(), whitelist, masks, maxLevel);
            Map<String, Object> before = project(e.before(), whitelist, masks, maxLevel);

            out.add(new RecordImpl(e.op(), e.key(), before, after, e.tso(), e.partition(),
                    e.offset(), stream, Mode.INCREMENTAL));
        }
        return out;
    }

    private Map<String, Object> project(Map<String, Object> row, Set<String> whitelist,
                                        Map<String, String> masks, int maxLevel) {
        if (row == null) return null;
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> en : row.entrySet()) {
            String col = en.getKey();
            if (whitelist != null && !whitelist.contains(col)) continue;   // 越权列在此丢弃
            Object v = en.getValue();
            String rule = masks.get(col);
            if (rule != null && masker != null) v = masker.mask(col, v, rule, maxLevel);
            out.put(col, v);
        }
        return out;
    }

    /** 无限制裁剪(仅用于未接权限引擎的场景):所有列/所有行都进逻辑。 */
    public static DefaultContractFilter permissive() {
        return new DefaultContractFilter((cond, row) -> true, (col, v, rule, lvl) -> v);
    }

    /** 取当前生效的契约视图(便于运行时要一份 ContractView)。 */
    public ContractView currentView() {
        if (fixedView != null) return fixedView;
        return new ContractView() {
            @Override public java.util.Set<String> columnWhitelist(String stream) { return null; }
            @Override public int maxLevel() { return -1; }
            @Override public Map<String, String> maskRules() { return Map.of(); }
            @Override public String rowCondition() { return null; }
        };
    }

    /** 固定列白名单的裁剪(测试常用)。 */
    public static DefaultContractFilter ofColumns(String... columns) {
        Set<String> set = Set.of(columns);
        ContractView view = new ContractView() {
            @Override public Set<String> columnWhitelist(String stream) { return set; }
            @Override public int maxLevel() { return -1; }
            @Override public Map<String, String> maskRules() { return Map.of(); }
            @Override public String rowCondition() { return null; }
        };
        return new DefaultContractFilter((cond, row) -> true, (col, v, rule, lvl) -> v, view);
    }
}

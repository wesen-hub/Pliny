package com.dcp.module.outlet;

import com.dcp.common.BizException;
import com.dcp.module.outlet.model.ChangeEvent;
import com.dcp.module.outlet.model.ConsumerAccount;
import com.dcp.module.outlet.model.Contract;
import com.dcp.module.outlet.perm.RowCondition;
import com.dcp.module.tagengine.perm.DataSecurityService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Predicate;

/**
 * 出口权限引擎(设计 §7.2 五道判定 / §7.3 四条铁律)。
 *
 * <p><b>每次读</b>都跑,顺序:① 流级 → ② 列级 → ③ 行级 → ④ 密级 → ⑤ 配额(配额见 {@link OutletQuotaLimiter})。
 * 实际授权 = **契约 ∩ 主体自身权限**(契约只能收窄,不能放大)。
 *
 * <p>铁律落地点在 {@link #projectEvent} / {@link #projectRow}:
 * 不在白名单的列**连 key 都不出现**;密级超限按 maskRules 脱敏或整列剔除;
 * 行条件不满足 → 事件标 {@code outOfScope}(等价删除)。
 *
 * <p>主体侧权限:若容器里存在 {@link DataSecurityService}(L0-L3),则按其列级判定再收窄一层;
 * 不存在时退化为"仅契约"(骨架环境)。真实实现应同时接入行规则与条件下推(见 §7.6)。
 */
@Slf4j
@Component
public class OutletPermissionEngine {

    /** 主体侧权限来源(L0-L3);骨架/单测环境下可为 null → 退化为“仅契约” */
    private final DataSecurityService dataSecurity;

    public OutletPermissionEngine(@Autowired(required = false) DataSecurityService dataSecurity) {
        this.dataSecurity = dataSecurity;
    }

    /** 一次读的投影计划(权限判定结果,供后续事件/行投影复用) */
    public static class ProjectionPlan {
        public Set<String> allowedColumns = new LinkedHashSet<>();
        public Map<String, String> maskRules = new LinkedHashMap<>();
        public Predicate<Map<String, Object>> rowFilter = r -> true;
        public List<String> trimmedColumns = new ArrayList<>();
        public List<String> maskedColumns = new ArrayList<>();
        /** 主体侧收窄掉的列(契约 ∩ 主体 的差额,审计里要能解释) */
        public List<String> subjectTrimmed = new ArrayList<>();
    }

    /** ① 流级:契约必须覆盖该流 */
    public void checkStream(ConsumerAccount consumer, Contract contract, String streamId) {
        if (!"active".equalsIgnoreCase(consumer.getStatus())) {
            throw new BizException(OutletErrorCode.CONTRACT_FORBIDDEN, "消费方非 active:" + consumer.getConsumerId());
        }
        if (!contract.active()) {
            throw new BizException(OutletErrorCode.CONTRACT_FORBIDDEN, "契约无效或已过期:" + contract.getContractId());
        }
        if (!contract.covers(streamId)) {
            throw new BizException(OutletErrorCode.STREAM_NOT_FOUND,
                    "契约 " + contract.getContractId() + " 未覆盖流 " + streamId);
        }
    }

    /**
     * ②③④ 生成投影计划。
     *
     * @param availableColumns 数据源侧可见列(真实实现来自表结构;用于计算 trimmedColumns)
     */
    public ProjectionPlan plan(ConsumerAccount consumer, Contract contract, String streamId,
                               Collection<String> availableColumns) {
        ProjectionPlan p = new ProjectionPlan();

        Set<String> whitelist = contract.getColumnWhitelist() == null
                ? null : contract.getColumnWhitelist().get(streamId);

        // 列级:契约白名单(为 null 表示契约未限定 → 仍受主体侧收窄)
        if (whitelist != null) {
            p.allowedColumns.addAll(whitelist);
        } else if (availableColumns != null) {
            p.allowedColumns.addAll(availableColumns);
        }

        // 主体侧收窄(契约 ∩ 主体):DataSecurityService 列级=HIDDEN 的直接剔除
        DataSecurityService dss = dataSecurity;
        if (dss != null && availableColumns != null) {
            for (String col : new ArrayList<>(p.allowedColumns)) {
                if (dss.columnLevel(streamId, col) == DataSecurityService.HIDDEN) {
                    p.allowedColumns.remove(col);
                    p.subjectTrimmed.add(col);
                }
            }
        }

        // 密级:超契约上限 → 按规则脱敏(无规则则剔除)
        if (contract.getMaskRules() != null) {
            contract.getMaskRules().forEach((col, rule) -> {
                if (p.allowedColumns.contains(col)) {
                    p.maskRules.put(col, rule);
                    p.maskedColumns.add(col);
                }
            });
        }

        // 行级:契约行条件(∩ 主体行规则 → 生产实现走条件下推,骨架仅做契约侧谓词)
        p.rowFilter = buildRowFilter(contract.getRowCondition());

        if (availableColumns != null) {
            for (String col : availableColumns) {
                if (whitelist != null && !whitelist.contains(col) && !p.trimmedColumns.contains(col)) {
                    p.trimmedColumns.add(col);
                }
            }
        }
        return p;
    }

    /** 投影一行:裁列 + 脱敏(快照段与增量段必须共用本方法,保证可见性一致) */
    public Map<String, Object> projectRow(ProjectionPlan plan, Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (!plan.allowedColumns.isEmpty() && !plan.allowedColumns.contains(e.getKey())) {
                continue;   // ★ 越权列连 key 都不出现
            }
            String mask = plan.maskRules.get(e.getKey());
            out.put(e.getKey(), mask == null ? e.getValue() : applyMask(mask, e.getValue()));
        }
        return out;
    }

    /** 投影事件:裁列 + 脱敏 + 行条件不满足 → outOfScope(条件退出) */
    public ChangeEvent projectEvent(ProjectionPlan plan, ChangeEvent e) {
        ChangeEvent out = new ChangeEvent();
        out.setOp(e.getOp());
        out.setStreamId(e.getStreamId());
        out.setTso(e.getTso());
        out.setEpoch(e.getEpoch());
        out.setTs(e.getTs());
        out.setKey(new LinkedHashMap<>(e.getKey()));
        out.setRow(projectRow(plan, e.getRow()));
        out.setBefore(e.getBefore() == null ? null : projectRow(plan, e.getBefore()));
        boolean inScope = plan.rowFilter.test(e.getRow() == null ? Map.of() : e.getRow());
        out.setOutOfScope(!inScope && !"DELETE".equals(e.getOp()));
        if (out.isOutOfScope()) {
            // 行仍存在但已不满足契约 → 对消费方等价删除(墓碑语义,设计 §5.5)
            out.setOp("DELETE");
        }
        return out;
    }

    /* ------------------------------------------------------------------ helpers */

    /**
     * 行条件 → 逐行谓词:委托 {@link RowCondition}。
     *
     * <p><b>语义变更(2026-09-16)</b>:原实现只支持 {@code col = value},且**解析不出来就静默放行**
     * (等于放大权限)。现改为完整语法 + **fail-closed**(非空但非法 → 明确报错,拒绝下发)。
     * 语法/语义与计算面 {@code ExpressionRowConditionEvaluator} 同源,由 parity 自检兜住。
     */
    private Predicate<Map<String, Object>> buildRowFilter(String condition) {
        try {
            return RowCondition.compile(condition);
        } catch (IllegalArgumentException e) {
            throw new BizException(OutletErrorCode.FORBIDDEN, "契约行条件非法:" + e.getMessage());
        }
    }

    private Object applyMask(String rule, Object value) {
        if (value == null) {
            return null;
        }
        String s = String.valueOf(value);
        return switch (rule) {
            case "MASK_PHONE" -> s.length() >= 7
                    ? s.substring(0, 3) + "****" + s.substring(s.length() - 2)
                    : "***";
            case "MASK_FULL" -> "***";
            default -> s.length() > 4 ? s.substring(0, 2) + "****" + s.substring(s.length() - 2) : "***";
        };
    }
}

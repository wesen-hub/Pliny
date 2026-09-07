package com.dcp.tagjob.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 单个资产(规则 或 规则流)的编译产物模型。
 * 规则资产: kind=rule, pkgName=独立包, drl 文本已含包声明/导入/动作函数头 + 规则体;
 *           agenda 过滤按「包名」放行(drools Rule.getPackageName), 避免手写 DRL 规则名不可预测的问题。
 * 规则流资产: kind=flow, steps 控制流(引用资产的顺序下标, driver 预解析)。
 */
public class AssetPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    public long id;
    public String kind;            // rule | flow
    public String name;
    public int ver;
    /** 规则资产: 该资产的 DRL 包名(dcp.tagjob.asset.a{id}_v{ver}) */
    public String pkgName;
    /** 规则资产: 完整 DRL 文本(含头部), 空表示该资产无规则(如异常守卫后的兜底, 正常不会出现) */
    public String drl;
    /** 规则流资产: 步骤(下标已解析为 assets 列表内索引) */
    public List<FlowStep> steps = new ArrayList<>();

    public static class FlowStep implements Serializable {
        private static final long serialVersionUID = 1L;
        /** 引用资产的顺序下标(直接可寻址, 避免 executor 二次查 id) */
        public int assetIdx;
        public long refId;
        public String refName;
        /** 命中分支: next|stop|jump(§3/§11①: stop 只终止当前流) */
        public String hit;
        public int hitJump;
        /** 未命中分支: next|end|jump */
        public String miss;
        public int missJump;
    }
}

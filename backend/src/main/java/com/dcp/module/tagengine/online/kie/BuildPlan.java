package com.dcp.module.tagengine.online.kie;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 执行计划(广播给 executor 的只读模型)。driver 完成:
 *   ① schema → Fact 字段表 ② DRL 文本生成/手写 DRL 收集 ③ Fact Java 源码生成 ④ driver 侧 KieBase 编译校验
 * executor 用同一模型: Fact 源码 + 每资产 DRL 重建成 KieModule/KieBase(每 executor 一次) + 逐分区求值
 * (设计文档 §12: 避免跨 JVM KieBase 反序列化依赖动态类的坑; Fact 源码入 KieModule 由 KieBuilder 编译,
 * 实例化用 moduleClassLoader —— 不依赖任何运行时动态类字节)。
 */
public class BuildPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 任务键 t{taskId}_v{taskVer} */
    public String taskKey;
    public long taskId;
    public int taskVer;

    /** Fact 类规格(Java 源码; 编译发生在 executor 本地 KieModule 构建) */
    public FactSpec factSpec;

    /** 全部参与资产(driver 已按 bindings/流引用排序收录), 数组下标即资产 id 索引 */
    public List<AssetPlan> assets = new ArrayList<>();

    /** 平铺规则执行序: 绑定中 kind=rule 按数组序(相对序保持 bindings 数组序) */
    public int[] flatOrder;
    /** 规则流执行序: 绑定中 kind=flow 按数组序 */
    public int[] flowOrder;

    // ------- 行级求值元数据(schema 真相, 见 PlanBuilder.analyzeSchema) -------
    /** Fact 字段(含派生; 按 df.schema 真实类型分类) */
    public List<FactField> factFields = new ArrayList<>();
    /** 实体键字段名 */
    public String entityField;

    public static class FactSpec implements Serializable {
        private static final long serialVersionUID = 1L;
        /** FQCN, 如 dcp.tagjob.fact.t1_v2.Fact */
        public String className;
        /** 简单名统一为 Fact(用户 DRL/生成 DRL 以 import 引用) */
        public String simpleName;
        /** Fact Java 源码(driver 生成; 与 DRL 一起写入 KieModule 由 KieBuilder 编译, 见 KieBaseFactory) */
        public String javaSource;
    }

    /** Fact 字段: name + 值分类(kind: L long / D double / B boolean / S string[含日期时间转字符串]) */
    public static class FactField implements Serializable {
        private static final long serialVersionUID = 1L;
        public String name;
        public char kind;

        public FactField() {
        }

        public FactField(String name, char kind) {
            this.name = name;
            this.kind = kind;
        }
    }
}

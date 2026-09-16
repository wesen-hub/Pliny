package com.dcp.tag.kie;

import java.util.List;

/**
 * 动态 Fact 类源码生成(设计文档 §12: Fact 类由 driver 按任务字段清单 + schema 类型动态生成并编译)。
 *
 * 约定:
 *  - 简单名固定为 Fact, 包名 dcp.tagjob.fact.t{taskId}_v{taskVer} —— 包级隔离避免任务间类名冲突
 *    (用户 DRL / 生成 DRL 通过 import FQCN 引用, 手写 DRL 里写 "Fact" 即解析到本类)
 *  - 每字段 private + public getter/setter(get{Field}); 包装类型(Long/Double/Boolean/String),
 *    保证 null 值与 DRL "field == null" 判空可表达
 *  - 值类型分类: L long → Long, D double(含 decimal 精度降级) → Double, B boolean → Boolean,
 *    S string(含 date/timestamp 序列化字符串) → String(见 PlanBuilder.analyzeSchema 注释)
 */
public final class FactSourceGen {

    private FactSourceGen() {
    }

    public static String packageOf(long taskId, int taskVer) {
        return "dcp.tagjob.fact.t" + taskId + "_v" + taskVer;
    }

    public static String className(long taskId, int taskVer) {
        return packageOf(taskId, taskVer) + ".Fact";
    }

    /** 生成 Java 源码 */
    public static String genSource(String pkg, List<BuildPlan.FactField> fields) {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("package ").append(pkg).append(";\n\n");
        sb.append("public class Fact {\n");
        for (BuildPlan.FactField f : fields) {
            sb.append("  private ").append(javaType(f.kind)).append(' ').append(f.name).append(";\n");
        }
        sb.append('\n');
        for (BuildPlan.FactField f : fields) {
            String cap = cap(f.name);
            String jt = javaType(f.kind);
            sb.append("  public ").append(jt).append(" get").append(cap).append("() { return ").append(f.name).append("; }\n");
            sb.append("  public void set").append(cap).append('(').append(jt).append(" v) { this.").append(f.name).append(" = v; }\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    /** 值分类 → Java 类型 */
    public static String javaType(char kind) {
        return switch (kind) {
            case 'L' -> "Long";
            case 'D' -> "Double";
            case 'B' -> "Boolean";
            case 'S' -> "String";
            default -> throw new IllegalArgumentException("未知 Fact 字段类型: " + kind);
        };
    }

    private static String cap(String name) {
        if (name.isEmpty()) return name;
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }
}

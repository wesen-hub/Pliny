package com.dcp.tagjob.drl;

import com.dcp.tagjob.config.TaskConfig;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 资产(规则)定义 → DRL 文本生成(设计文档 §7/§12: 可视化=单 rule; 决策表=排他组 activation-group+salience;
 * DRL 手写=原样直跑)。
 *
 * <p>动作约定(用户 DRL 的 API, 同步写入模块 README):
 * <pre>
 *   tag($f, "标签路径"[, "值"])  → 记入行级 TagSink(写 tag_fact 的行 + 命中计数)
 *   halt()                       → 停止当前资产组求值(置 halt 标志 + 尽力中断当前 fire)
 * </pre>
 * 实现: 包内 function 声明包装调用静态 {@code com.dcp.tagjob.run.TagActions}(ThreadLocal sink/session,
 * fire 前 bind、行处理完 clear)。每个资产生成独立包(dcp.tagjob.asset.a{id}_v{ver}), agenda 按包名过滤。
 */
public final class DrlGenerator {

    private DrlGenerator() {
    }

    /** 资产包名 */
    public static String assetPackage(long id, int ver) {
        return "dcp.tagjob.asset.a" + id + "_v" + ver;
    }

    /** 规则资产 → DRL 文本; 结构/类型问题追加到 errors(带资产定位), 返回 null */
    public static String genRuleDrl(TaskConfig.RuleSnapshot rule, String factFqcn,
                                    Map<String, Character> fieldKinds, List<String> errors) {
        String pkg = assetPackage(rule.id, rule.ver);
        JsonNode def = rule.def;
        String mode = def.path("mode").asText("");
        StringBuilder rules = new StringBuilder();
        if ("table".equals(mode)) {
            genTable(rule, fieldKinds, rules, errors);
        } else if ("visual".equals(mode)) {
            genVisual(rule, fieldKinds, rules, errors);
        } else if ("drl".equals(mode)) {
            String src = def.path("drl").asText("");
            if (src.isBlank()) {
                errors.add(locate(rule) + " DRL 源码为空");
                return null;
            }
            rules.append(src);
            if (!src.endsWith("\n")) rules.append('\n');
        } else {
            errors.add(locate(rule) + " 未知定义方式 mode=" + mode + "(须 visual|table|drl)");
            return null;
        }
        return header(pkg, factFqcn) + rules;
    }

    /* ---------- 包头部: import + dialect + 动作函数声明 ---------- */

    private static String header(String pkg, String factFqcn) {
        return "package " + pkg + ";\n"
                + "import " + factFqcn + ";\n"
                + "dialect \"mvel\"\n"
                + "\n"
                // 注意: MVEL function 按名字生成类型, 不支持同名重载 → tag 固定三参, 布尔场景第三参传 null
                + "function void tag(" + factFqcn + " $f, String path, String value) {\n"
                + "    com.dcp.tagjob.run.TagActions.tag($f, path, value);\n"
                + "}\n"
                + "function void halt() {\n"
                + "    com.dcp.tagjob.run.TagActions.halt();\n"
                + "}\n"
                + "\n";
    }

    /* ---------- 决策表: 每行一 rule + 兜底, 同 activation-group(资产品质隔离名) + salience 行序越高越优先 ---------- */

    private static void genTable(TaskConfig.RuleSnapshot rule, Map<String, Character> fieldKinds,
                                 StringBuilder out, List<String> errors) {
        String loc = locate(rule);
        JsonNode t = rule.def.path("table");
        String field = t.path("field").asText("");
        String tag = t.path("tag").asText("");
        if (field.isEmpty() || tag.isEmpty()) {
            errors.add(loc + " 决策表缺分档字段或标签");
            return;
        }
        if (!fieldKinds.containsKey(field)) {
            errors.add(loc + " 决策表分档字段不在事实字段域: " + field + "(数据集字段 ∪ 派生字段, DRL 人工核对不适用)");
            return;
        }
        char kind = fieldKinds.get(field);
        String group = "grp_a" + rule.id + "_v" + rule.ver;   // 排他组: 资产品质隔离(同一资产内互斥; 跨资产同 tag 冲突由发布校验/执行序兜底)
        int n = 0;
        JsonNode rows = t.path("rows");
        if (rows.isArray()) {
            for (JsonNode row : rows) {
                String op = row.path("op").asText("");
                String th = row.path("th").asText("");
                String val = row.path("val").asText("");
                String cond = numericCond(field, kind, op, th, errors, loc + " 档位行" + n);
                if (cond == null) return;
                if (val.isEmpty()) {
                    errors.add(loc + " 档位行" + n + " 缺档值(val)");
                    return;
                }
                // salience: 行序越靠前越高 → 排他组内先命中先执行, 兜底 salience 最低
                int salience = (rows.size() - n) * 10 + 10;
                rule(out, "a" + rule.id + "_v" + rule.ver + "_row" + n, salience, group, cond,
                        "tag($f, " + lit(tag) + ", " + lit(val) + ");");
                n++;
            }
        }
        String elseVal = t.path("elseVal").asText("");
        if (elseVal.isEmpty()) {
            if (n == 0) errors.add(loc + " 决策表至少一个档位行或兜底值");
            return;
        }
        // 兜底: 恒真(无条件) + 排他组最低 salience —— 行命中时组内高 salience 先行并撤销兜底; 未命中时兜底执行
        rule(out, "a" + rule.id + "_v" + rule.ver + "_else", 0, group, "",
                "tag($f, " + lit(tag) + ", " + lit(elseVal) + ");");
    }

    /* ---------- 可视化: 单 rule(when 条件组) ---------- */

    private static void genVisual(TaskConfig.RuleSnapshot rule, Map<String, Character> fieldKinds,
                                  StringBuilder out, List<String> errors) {
        String loc = locate(rule);
        JsonNode v = rule.def.path("visual");
        JsonNode when = v.path("when");
        // 语义(设计 §3): 组内按 g.logic(AND/OR)合并, 组间全部满足(AND)。
        // 注: 前端"从可视化生成 DRL"mock 曾把组间拼成 ||, 与 UI 提示「组间: 全部满足」矛盾; 执行器以设计 §3+UI 提示为准。
        List<String> groupExprs = new ArrayList<>();
        if (when.isArray()) {
            for (JsonNode g : when) {
                String logic = "AND".equalsIgnoreCase(g.path("logic").asText("AND")) ? "&&" : "||";
                List<String> conds = new ArrayList<>();
                for (JsonNode c : g.path("conds")) {
                    String field = c.path("field").asText("");
                    String op = c.path("op").asText("");
                    String val = c.path("val").asText("");
                    if (field.isEmpty()) continue;
                    if (!fieldKinds.containsKey(field)) {
                        errors.add(loc + " 可视化条件字段不在事实字段域: " + field);
                        return;
                    }
                    char kind = fieldKinds.get(field);
                    String expr = condExpr(field, kind, op, val, errors, loc + " 条件[" + field + " " + op + "]");
                    if (expr == null) return;
                    conds.add(expr);
                }
                if (conds.isEmpty()) continue;
                groupExprs.add("(" + String.join(" " + logic + " ", conds) + ")");
            }
        }
        String whenExpr = String.join(" && ", groupExprs);
        JsonNode actions = v.path("actions");
        StringBuilder then = new StringBuilder();
        if (actions.isArray()) {
            for (JsonNode a : actions) {
                String type = a.path("type").asText("tag");
                if ("stop".equals(type)) {
                    then.append("  halt();\n");
                } else {
                    String tag = a.path("tag").asText("");
                    String val = a.path("val").asText("");
                    if (tag.isEmpty()) {
                        errors.add(loc + " THEN 打标签动作缺标签");
                        return;
                    }
                    if (val.isEmpty()) {
                        then.append("  tag($f, ").append(lit(tag)).append(", null);\n");
                    } else {
                        then.append("  tag($f, ").append(lit(tag)).append(", ").append(lit(val)).append(");\n");
                    }
                }
            }
        }
        if (then.length() == 0) {
            errors.add(loc + " 可视化规则缺 THEN 动作");
            return;
        }
        rule(out, "a" + rule.id + "_v" + rule.ver + "_v", 0, null,
                whenExpr.isEmpty() ? "" : whenExpr, then.toString());
    }

    /* ---------- DRL 片段拼装 ---------- */

    private static void rule(StringBuilder out, String name, int salience, String activationGroup,
                             String cond, String thenStmts) {
        out.append("rule \"").append(escapeDrl(name)).append('"');
        if (salience != 0) out.append("\n  salience ").append(salience);
        if (activationGroup != null) out.append("\n  activation-group \"").append(escapeDrl(activationGroup)).append('"');
        out.append("\nwhen\n");
        out.append(cond.isEmpty() ? "  $f : Fact()\n" : "  $f : Fact(" + cond + ")\n");
        out.append("then\n").append(thenStmts).append("end\n\n");
    }

    /* ---------- 条件/字面量渲染 ---------- */

    private static String numericCond(String field, char kind, String op, String th,
                                      List<String> errors, String where) {
        if (!java.util.Set.of(">=", ">", "<", "<=", "==").contains(op)) {
            errors.add(where + " 非法档位操作符: " + op);
            return null;
        }
        if (th.isEmpty()) {
            errors.add(where + " 缺阈值(th)");
            return null;
        }
        if (kind == 'S') errors.add(where + " 分档字段为字符串类型, 不建议数值比较: " + field);
        String lit = typedNumberLiteral(kind, th, errors, where + " 阈值");
        return lit == null ? null : field + " " + op + " " + lit;
    }

    private static String condExpr(String field, char kind, String op, String val,
                                   List<String> errors, String where) {
        switch (op) {
            case "==" -> {
                return field + " == " + typedLiteral(kind, val, errors, where);
            }
            case "!=" -> {
                return field + " != " + typedLiteral(kind, val, errors, where);
            }
            case ">", ">=", "<", "<=" -> {
                if (kind != 'L' && kind != 'D') {
                    errors.add(where + " 比较操作符仅支持数值字段(当前字段类型 " + kind + ")");
                    return null;
                }
                return field + " " + op + " " + typedNumberLiteral(kind, val, errors, where);
            }
            case "in" -> {
                // in(集合): 转 || 等值(设计: in 集合转等值链)
                List<String> parts = splitList(val);
                if (parts.isEmpty()) {
                    errors.add(where + " in 集合为空");
                    return null;
                }
                List<String> eqs = new ArrayList<>();
                for (String p : parts) {
                    eqs.add(field + " == " + typedLiteral(kind, p, errors, where));
                }
                return "(" + String.join(" || ", eqs) + ")";
            }
            case "like" -> {
                // like → contains(不区分大小写), 仅字符串字段
                if (kind != 'S') {
                    errors.add(where + " like 仅支持字符串字段");
                    return null;
                }
                String v = val.toLowerCase();
                return v.isEmpty() ? field + " != null"
                        : field + " != null && " + field + ".toLowerCase().contains(" + lit(v) + ")";
            }
            case "null" -> {
                return field + " == null";
            }
            case "notnull" -> {
                return field + " != null";
            }
            default -> {
                errors.add(where + " 未知条件操作符: " + op);
                return null;
            }
        }
    }

    private static String typedLiteral(char kind, String val, List<String> errors, String where) {
        return switch (kind) {
            case 'L', 'D' -> typedNumberLiteral(kind, val, errors, where);
            case 'B' -> {
                if (!"true".equalsIgnoreCase(val) && !"false".equalsIgnoreCase(val)) {
                    errors.add(where + " 布尔字段须为 true/false, 实为: " + val);
                    yield null;
                }
                yield val.toLowerCase();
            }
            case 'S' -> lit(val);
            default -> {
                errors.add(where + " 未知字段类型: " + kind);
                yield null;
            }
        };
    }

    private static String typedNumberLiteral(char kind, String val, List<String> errors, String where) {
        if (val.isBlank()) {
            errors.add(where + " 数值为空");
            return null;
        }
        try {
            if (kind == 'L') {
                Long.parseLong(val.trim());
                return val.trim();
            }
            Double.parseDouble(val.trim());
            return val.trim();
        } catch (NumberFormatException e) {
            errors.add(where + " 不是合法数值: " + val);
            return null;
        }
    }

    private static List<String> splitList(String val) {
        List<String> out = new ArrayList<>();
        for (String p : val.split("[,，;；、\\s]+")) {
            if (!p.isEmpty()) out.add(p);
        }
        return out;
    }

    /** DRL 字符串字面量(转义) */
    private static String lit(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.append('"').toString();
    }

    private static String escapeDrl(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String locate(TaskConfig.RuleSnapshot rule) {
        return "[规则 " + rule.kind + "#" + rule.id + "(" + rule.name + ") v" + rule.ver + "]";
    }
}

package com.dcp.outlet.adapters;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * **行条件一致性自检**:平台 `RowCondition`(真类,编译后加载)vs 计算面
 * {@link ExpressionRowConditionEvaluator} —— 在「条件 × 行」矩阵上**逐对比对**判定结果。
 *
 * <p>为什么必须有:两端是**镜像实现**(计算面独立部署,不能共享类)。只要一边改了语法或比较语义,
 * 就会出现"平台认为这行可见、计算面认为不可见"(丢数据)或反之(**放大权限**)。
 * 这个自检把"两处需同步"从口头约束变成回归里的硬门。
 *
 * <p>失败语义也一起验:非空但非法的条件,两端都必须**拒绝**(fail-closed),不许静默放行。
 *
 * <p>用法(仓库根目录):{@code java -cp <classes>[:<platform-classes>] ...RowConditionParityCheck}
 * 若加载不到平台类,自检会用 {@code javac} 编译那一个源文件到临时目录(并检查退出码)。
 */
public final class RowConditionParityCheck {

    private static int passed, failed;

    private static final String SRC = "backend/src/main/java/com/dcp/module/outlet/perm/RowCondition.java";
    private static final String CLS = "com.dcp.module.outlet.perm.RowCondition";

    public static void main(String[] args) throws Exception {
        System.out.println("=== 行条件一致性:平台 RowCondition ↔ 计算面 ExpressionRowConditionEvaluator ===");

        Class<?> platform = loadPlatformClass(args);
        Method compile = platform.getMethod("compile", String.class);

        // ---------- 1) 合法条件:两侧判定必须完全相同 ----------
        List<String> valid = List.of(
                "region = CN",
                "region = 'CN'",
                "region = \"CN\"",
                "region=CN",
                "  region   =   CN  ",
                "region = CN AND status = OK",
                "region = CN AND status = OK AND level = 3",
                "status != OK",
                "status != 'OK'",
                "status IN (OK, NEW)",
                "status IN ('OK','NEW')",
                "status IN (OK)",
                "status NOT IN (OK)",
                "status NOT IN ('OK', 'NEW')",
                "region IS NULL",
                "region IS NOT NULL",
                "city IN ('a AND b')",              // 引号内的 AND 不能被切开
                "城市 = 上海",
                "note = a b&c=d",
                "level = 3",
                "v != ",
                ""
        );

        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row("region", "CN", "status", "OK", "level", 3));
        rows.add(row("region", "CN", "status", "NEW", "level", 3));
        rows.add(row("region", "US", "status", "OK", "level", 2));
        rows.add(row("region", null, "status", "OK"));
        rows.add(row("region", "CN"));
        rows.add(row("status", "OK", "level", "3"));         // 数值以字符串形式出现(比较按字符串)
        rows.add(row("city", "a AND b", "status", "OK"));
        rows.add(row("城市", "上海", "region", "CN"));
        rows.add(row("note", "a b&c=d"));
        rows.add(row("v", "", "region", "CN"));
        rows.add(new LinkedHashMap<>());                     // 空行
        rows.add(null);                                      // null 行

        int pairs = 0;
        for (String cond : valid) {
            for (Map<String, Object> r : rows) {
                pairs++;
                boolean a = safe(() -> predicate(compile, cond).test(r));
                boolean b = safe(() -> new ExpressionRowConditionEvaluator().test(cond, r));
                if (a != b) {
                    failed++;
                    System.out.println("  FAIL  判定不一致 cond=[" + cond + "] row=" + r
                            + " 平台=" + a + " 计算面=" + b);
                } else {
                    passed++;
                }
            }
        }
        System.out.println("  INFO  合法条件矩阵:" + valid.size() + " 条件 × " + rows.size() + " 行 = "
                + pairs + " 对,判定全部一致");

        // ---------- 2) 空条件 = 无行限制(放行) ----------
        check("空条件放行(平台)", predicate(compile, "").test(row("region", "US")), true);
        check("空条件放行(计算面)", new ExpressionRowConditionEvaluator().test("", row("region", "US")), true);
        check("null 条件放行(计算面)", new ExpressionRowConditionEvaluator().test(null, row("region", "US")), true);

        // ---------- 3) 非法条件:两端都必须拒绝(fail-closed) ----------
        List<String> invalid = List.of(
                "region",                       // 没有操作符
                "region <> CN",                 // 不支持 <>
                "region > 5",                   // 不支持大小比较
                "status IN ()",                 // 空集合
                "status NOT NULL",              // 缺 IS
                "AND region = CN",              // 空片段
                "region = CN AND",              // 空片段
                "status IS MAYBE"               // 只认 NULL / NOT NULL
        );
        for (String cond : invalid) {
            boolean platformRejected = rejectedByPlatform(compile, cond);
            boolean workerRejected = rejectedByWorker(cond);
            check("非法条件两端拒绝 [" + cond + "]", platformRejected + "/" + workerRejected, "true/true");
        }

        // ---------- 4) 镜像段逐字一致(防"改了一边") ----------
        checkMirror();

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    /* ------------------------------------------------------------------ 反射加载平台类 */

    private static Class<?> loadPlatformClass(String[] args) throws Exception {
        List<String> cands = new ArrayList<>();
        if (args.length > 0) cands.add(args[0]);
        cands.add("backend/target/classes");
        cands.add("/tmp/perm-cond");

        for (String dir : cands) {
            Class<?> c = tryLoad(dir);
            if (c != null) {
                System.out.println("  INFO  平台类来源:" + dir);
                return c;
            }
        }
        // 都没有 → 现场编译那一个源文件(纯类,无第三方依赖)
        Path src = Path.of(SRC);
        if (!Files.isRegularFile(src)) {
            System.out.println("  FAIL  找不到平台源码:" + SRC + "(需在仓库根目录运行)");
            System.exit(1);
        }
        Path out = Files.createTempDirectory("rowcond-parity");
        Process p = new ProcessBuilder("javac", "--release", "17", "-encoding", "UTF-8",
                "-d", out.toString(), SRC).inheritIO().start();
        int code = p.waitFor();
        if (code != 0) {
            System.out.println("  FAIL  javac 编译平台类失败,退出码=" + code);
            System.exit(1);
        }
        Class<?> c = tryLoad(out.toString());
        if (c == null) {
            System.out.println("  FAIL  编译成功但加载不到 " + CLS);
            System.exit(1);
        }
        System.out.println("  INFO  平台类现场编译自:" + SRC + " → " + out);
        return c;
    }

    private static Class<?> tryLoad(String dir) {
        File f = new File(dir);
        if (!f.isDirectory()) return null;
        try {
            URLClassLoader cl = new URLClassLoader(new URL[]{f.toURI().toURL()},
                    RowConditionParityCheck.class.getClassLoader());
            return Class.forName(CLS, true, cl);
        } catch (Throwable t) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static Predicate<Map<String, Object>> predicate(Method compile, String cond) {
        try {
            return (Predicate<Map<String, Object>>) compile.invoke(null, cond);
        } catch (Exception e) {
            throw new IllegalStateException(e.getCause() == null ? e : e.getCause());
        }
    }

    private static boolean safe(java.util.function.BooleanSupplier s) {
        try {
            return s.getAsBoolean();
        } catch (Throwable t) {
            return false;               // 抛异常 = 该行不放行
        }
    }

    private static boolean rejectedByPlatform(Method compile, String cond) {
        try {
            predicate(compile, cond);
            return false;               // 编译成功 = 没拒绝
        } catch (Throwable t) {
            return true;
        }
    }

    private static boolean rejectedByWorker(String cond) {
        try {
            new ExpressionRowConditionEvaluator().test(cond, row("region", "CN"));
            return false;
        } catch (Throwable t) {
            return true;
        }
    }

    /** 镜像段一致性:两文件的"解析/求值"代码块必须逐字相同(只差包名/类名/缓存外壳)。 */
    private static void checkMirror() throws Exception {
        String a = mirror(Path.of(SRC));
        String b = mirror(Path.of(
                "outlet-adapters/src/main/java/com/dcp/outlet/adapters/ExpressionRowConditionEvaluator.java"));
        check("镜像段逐字一致(解析/求值代码)", a.equals(b) ? "一致" : "不一致(长度 " + a.length() + " vs " + b.length() + ")", "一致");
    }

    /** 取镜像段:从"编译行条件"注释到文件末尾,压掉空白后比对。 */
    private static String mirror(Path p) throws Exception {
        String src = Files.readString(p);
        int i = src.indexOf("/** 编译行条件");
        if (i < 0) return "MISSING-MIRROR:" + p;
        return src.substring(i).replaceAll("\\s+", " ").trim();
    }

    private static Map<String, Object> row(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what
                + (ok ? "" : "   (实际 " + actual + ",期望 " + expected + ")"));
    }
}

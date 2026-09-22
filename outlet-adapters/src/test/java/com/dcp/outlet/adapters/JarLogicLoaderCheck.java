package com.dcp.outlet.adapters;

import com.dcp.outlet.sdk.*;
import com.dcp.outlet.sdk.testkit.*;

import java.nio.file.*;
import java.util.List;
import java.util.Map;

/** 逻辑仓库 jar 加载:打包→定位→哈希校验→独立 ClassLoader→运行→释放;含篡改与缺失。 */
public final class JarLogicLoaderCheck {

    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        Path work = Files.createTempDirectory("logic-repo-check");
        Path src = work.resolve("src/com/acme");
        Files.createDirectories(src);
        Path classes = work.resolve("classes");
        Files.createDirectories(classes);

        // ① 造一个"业务逻辑"源码:只依赖 SDK
        Files.writeString(src.resolve("ToyLogic.java"), """
                package com.acme;
                import com.dcp.outlet.sdk.*;
                public class ToyLogic implements ConsumerLogic {
                    public int initCount, closeCount, rows;
                    @Override public void init(LogicInitContext ctx) { initCount++; }
                    @Override public void process(Batch b, ConsumerContext ctx) { rows += b.size(); }
                    @Override public void close() { closeCount++; }
                }
                """);
        Files.writeString(src.resolve("ToyLogicProvider.java"), """
                package com.acme;
                import com.dcp.outlet.sdk.*;
                public class ToyLogicProvider implements ConsumerLogicProvider {
                    @Override public java.util.List<ProviderEntry> logics() {
                        return java.util.List.of(ProviderEntry.of("toy-logic", "1.0.0", new ToyLogic()));
                    }
                }
                """);

        // ② 编译 + 打 SPI 声明 + 打包
        run(work, "javac", "-cp", System.getProperty("java.class.path"), "-d", classes.toString(),
                src.resolve("ToyLogic.java").toString(), src.resolve("ToyLogicProvider.java").toString());
        Path spiDir = classes.resolve("META-INF/services");
        Files.createDirectories(spiDir);
        Files.writeString(spiDir.resolve("com.dcp.outlet.sdk.ConsumerLogicProvider"), "com.acme.ToyLogicProvider\n");

        Path repoRoot = work.resolve("repo");
        Path artifact = repoRoot.resolve("toy-logic/1.0.0.jar");
        Files.createDirectories(artifact.getParent());
        run(work, "jar", "--create", "--file", artifact.toString(), "-C", classes.toString(), ".");

        // ③ 写哈希 sidecar(发布流程的动作)
        Files.writeString(artifact.resolveSibling("1.0.0.jar.sha256"),
                FileLogicRepository.sha256(artifact) + "  1.0.0.jar\n");

        // ④ 加载并运行
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(repoRoot));
        LoadedLogic loaded = loader.load(LogicRef.of("toy-logic", "1.0.0"));
        check("从 jar 加载成功", loaded.logic() != null, true);
        check("类来自 jar(不是 classpath)", loaded.logic().getClass().getName(), "com.acme.ToyLogic");
        check("类加载器隔离(独立 ClassLoader)",
                loaded.logic().getClass().getClassLoader() != ConsumerLogic.class.getClassLoader(), true);

        loaded.logic().init(FakeInitContext.of(Map.of()));
        loaded.logic().process(FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1L).key("id", "u-1").after("id", "u-1")), FakeConsumerContext.of(Mode.INCREMENTAL, 0));
        Object rows = loaded.logic().getClass().getField("rows").get(loaded.logic());
        check("jar 内逻辑真的跑起来(处理 1 行)", rows, 1);
        loaded.close();
        check("close() 正常释放", true, true);

        // ④-b 缺哈希 sidecar → fail-closed 拒绝(不告警跳过)
        Path noHashRepo = work.resolve("repo-nohash");
        Path noHashJar = noHashRepo.resolve("toy-logic/1.0.0.jar");
        Files.createDirectories(noHashJar.getParent());
        Files.copy(artifact, noHashJar);
        boolean missingHashRejected = false;
        try {
            new JarLogicLoader(new FileLogicRepository(noHashRepo)).load(LogicRef.of("toy-logic", "1.0.0"));
        } catch (IllegalStateException e) {
            missingHashRejected = e.getMessage() != null && e.getMessage().contains("未提供 sha256");
        }
        check("缺哈希 sidecar → 拒绝加载(fail-closed)", missingHashRejected, true);

        // ⑤ 篡改 → 拒绝加载
        Files.write(artifact, new byte[]{1, 2, 3}, StandardOpenOption.APPEND);
        boolean rejected = false;
        try {
            new JarLogicLoader(new FileLogicRepository(repoRoot)).load(LogicRef.of("toy-logic", "1.0.0"));
        } catch (IllegalStateException e) {
            rejected = e.getMessage() != null && e.getMessage().contains("哈希不匹配");
        }
        check("哈希不匹配 → 拒绝加载", rejected, true);

        // ⑥ 版本不存在 → LogicNotFoundException(平台错误码 LOGIC_NOT_AVAILABLE)
        boolean notFound = false;
        try {
            loader.load(LogicRef.of("toy-logic", "9.9.9"));
        } catch (LogicNotFoundException e) {
            notFound = true;
        }
        check("版本不存在 → 明确报错(不静默)", notFound, true);

        deleteRecursively(work);
        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void run(Path cwd, String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).directory(cwd.toFile()).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        int code = p.waitFor();
        if (code != 0) throw new IllegalStateException("命令失败(" + code + "): " + String.join(" ", cmd) + "\n" + out);
    }

    private static void deleteRecursively(Path p) throws Exception {
        if (!Files.exists(p)) return;
        try (var walk = Files.walk(p)) {
            walk.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(x -> {
                try { Files.deleteIfExists(x); } catch (Exception ignored) { }
            });
        }
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}

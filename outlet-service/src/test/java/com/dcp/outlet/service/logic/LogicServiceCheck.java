package com.dcp.outlet.service.logic;

import com.dcp.outlet.adapters.FileLogicRepository;
import com.dcp.outlet.adapters.JarLogicLoader;
import com.dcp.outlet.control.LogicCatalog;
import com.dcp.outlet.control.LogicInfo;
import com.dcp.outlet.control.LogicPublishException;
import com.dcp.outlet.sdk.LogicRef;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * LogicService 桩测试(离线,不碰真实数据库):覆盖发布成功 + 各拒绝路径
 * (版本冲突 / 缺 SPI 声明 / 声明不一致 / 试加载失败 / 非法 jar / 缺哈希 fail-closed)。
 */
public final class LogicServiceCheck {

    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        Path work = Files.createTempDirectory("logic-service-check");

        Path goodJar = buildGoodJar(work);          // SPI 声明 toy-logic@1.0.0
        Path noSpiJar = buildNoSpiJar(work);        // 无 META-INF/services
        Path brokenJar = buildBrokenJar(work);      // provider.logics() 抛异常

        StubCatalog catalog = new StubCatalog();
        // DataSource 传 null:发布拒绝路径不查 consumer_task(引用检查才用,不在本测试范围)
        LogicService svc = new LogicService(catalog, null, new FileArtifactStore(work.resolve("repo")));

        // ① 发布成功
        LogicService.PublishResult r = svc.publish(Files.readAllBytes(goodJar), "toy-logic", "1.0.0", "d", "wesen");
        check("发布成功", r.logicId() + "@" + r.version(), "toy-logic@1.0.0");
        check("发布登记入库", catalog.find("toy-logic", "1.0.0").isPresent(), true);
        check("登记哈希非空", catalog.find("toy-logic", "1.0.0").get().artifactHash() != null, true);
        check("发布审计已写", catalog.auditCount(), 1);

        // ② 版本冲突 → 拒绝(禁止覆盖)
        boolean dup = false;
        try {
            svc.publish(Files.readAllBytes(goodJar), "toy-logic", "1.0.0", "d", "wesen");
        } catch (LogicPublishException e) {
            dup = e.getMessage().contains("禁止覆盖");
        }
        check("版本冲突 → 拒绝", dup, true);

        // ③ 缺 SPI 声明 → 拒绝
        boolean noSpi = false;
        try {
            svc.publish(Files.readAllBytes(noSpiJar), "toy-logic", "2.0.0", "d", "wesen");
        } catch (LogicPublishException e) {
            noSpi = e.getMessage().contains("SPI");
        }
        check("缺 SPI 声明 → 拒绝", noSpi, true);

        // ④ 声明不一致(SPI 声明 1.0.0 但上传 9.9.9)→ 拒绝
        boolean mismatch = false;
        try {
            svc.publish(Files.readAllBytes(goodJar), "toy-logic", "9.9.9", "d", "wesen");
        } catch (LogicPublishException e) {
            mismatch = e.getMessage().contains("SPI");
        }
        check("SPI 声明不一致 → 拒绝", mismatch, true);

        // ⑤ 试加载失败(provider.logics() 抛异常)→ 拒绝
        boolean broken = false;
        try {
            svc.publish(Files.readAllBytes(brokenJar), "toy-logic", "3.0.0", "d", "wesen");
        } catch (LogicPublishException e) {
            broken = e.getMessage().contains("试加载预检失败");
        }
        check("试加载失败 → 拒绝", broken, true);

        // ⑥ 非法 jar(魔数不符)→ 拒绝
        boolean badMagic = false;
        try {
            svc.publish(new byte[]{1, 2, 3, 4}, "toy-logic", "4.0.0", "d", "wesen");
        } catch (LogicPublishException e) {
            badMagic = e.getMessage().contains("魔数");
        }
        check("非法 jar(魔数不符)→ 拒绝", badMagic, true);

        // ⑦ 缺哈希 fail-closed(加载端闭环,发布依赖此保证)
        Path noHashRepo = work.resolve("repo-nohash");
        Path noHashJar = noHashRepo.resolve("toy-logic/1.0.0.jar");
        Files.createDirectories(noHashJar.getParent());
        Files.copy(goodJar, noHashJar);
        boolean noHash = false;
        try {
            new JarLogicLoader(new FileLogicRepository(noHashRepo)).load(LogicRef.of("toy-logic", "1.0.0"));
        } catch (IllegalStateException e) {
            noHash = e.getMessage() != null && e.getMessage().contains("未提供 sha256");
        }
        check("缺哈希 → fail-closed 拒绝", noHash, true);

        deleteRecursively(work);
        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    /* ------------------------------------------------ 造 jar */

    private static Path buildGoodJar(Path work) throws Exception {
        Path src = work.resolve("good-src/com/acme");
        Files.createDirectories(src);
        Files.writeString(src.resolve("ToyLogic.java"), """
                package com.acme;
                import com.dcp.outlet.sdk.*;
                public class ToyLogic implements ConsumerLogic {
                    @Override public void init(LogicInitContext ctx) {}
                    @Override public void process(Batch b, ConsumerContext ctx) {}
                    @Override public void close() {}
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
        return compileAndJar(work, "good", src, "com.acme.ToyLogicProvider");
    }

    private static Path buildNoSpiJar(Path work) throws Exception {
        Path src = work.resolve("nospi-src/com/acme");
        Files.createDirectories(src);
        Files.writeString(src.resolve("ToyLogic.java"), """
                package com.acme;
                import com.dcp.outlet.sdk.*;
                public class ToyLogic implements ConsumerLogic {
                    @Override public void init(LogicInitContext ctx) {}
                    @Override public void process(Batch b, ConsumerContext ctx) {}
                    @Override public void close() {}
                }
                """);
        // 只编译类,不写 META-INF/services → ServiceLoader 发现不到
        return compileAndJar(work, "nospi", src, null);
    }

    private static Path buildBrokenJar(Path work) throws Exception {
        Path src = work.resolve("broken-src/com/acme");
        Files.createDirectories(src);
        Files.writeString(src.resolve("BrokenProvider.java"), """
                package com.acme;
                import com.dcp.outlet.sdk.*;
                public class BrokenProvider implements ConsumerLogicProvider {
                    @Override public java.util.List<ProviderEntry> logics() {
                        throw new RuntimeException("boom");
                    }
                }
                """);
        return compileAndJar(work, "broken", src, "com.acme.BrokenProvider");
    }

    private static Path compileAndJar(Path work, String name, Path src, String spiProvider) throws Exception {
        Path classes = work.resolve(name + "-classes");
        Files.createDirectories(classes);
        List<String> cmd = new ArrayList<>(List.of("javac", "-cp", System.getProperty("java.class.path"),
                "-d", classes.toString()));
        try (var s = Files.list(src)) {
            s.map(Path::toString).forEach(cmd::add);
        }
        run(work, cmd.toArray(String[]::new));

        if (spiProvider != null) {
            Path spiDir = classes.resolve("META-INF/services");
            Files.createDirectories(spiDir);
            Files.writeString(spiDir.resolve("com.dcp.outlet.sdk.ConsumerLogicProvider"), spiProvider + "\n");
        }
        Path jar = work.resolve(name + ".jar");
        run(work, "jar", "--create", "--file", jar.toString(), "-C", classes.toString(), ".");
        return jar;
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

    /* ------------------------------------------------ 桩目录 */

    static final class StubCatalog implements LogicCatalog {
        private final Map<String, LogicInfo> m = new LinkedHashMap<>();
        private final List<String> audit = new ArrayList<>();

        @Override public boolean registered(String a, String b) { return m.containsKey(k(a, b)); }
        @Override public List<LogicInfo> versionsOf(String id) {
            List<LogicInfo> out = new ArrayList<>();
            for (LogicInfo li : m.values()) if (li.logicId().equals(id)) out.add(li);
            return out;
        }
        @Override public List<LogicInfo> all() { return new ArrayList<>(m.values()); }
        @Override public Optional<LogicInfo> find(String a, String b) { return Optional.ofNullable(m.get(k(a, b))); }
        @Override public void register(LogicInfo info) {
            if (m.containsKey(k(info.logicId(), info.version()))) {
                throw new LogicPublishException("版本已存在,禁止覆盖:" + info.logicId() + "@" + info.version());
            }
            m.put(k(info.logicId(), info.version()), info);
        }
        @Override public void setStatus(String a, String b, String s) {
            LogicInfo old = m.get(k(a, b));
            if (old != null) {
                m.put(k(a, b), LogicInfo.builder(a, b).status(s)
                        .artifactUri(old.artifactUri()).artifactHash(old.artifactHash())
                        .publishedBy(old.publishedBy()).jarSize(old.jarSize()).preflight(old.preflight()).build());
            }
        }
        @Override public void audit(String a, String b, String act, String op, String d) { audit.add(act); }

        int auditCount() { return audit.size(); }
        private static String k(String a, String b) { return a + "@" + b; }
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}

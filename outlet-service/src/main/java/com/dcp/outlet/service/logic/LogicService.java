package com.dcp.outlet.service.logic;

import com.dcp.outlet.adapters.FileLogicRepository;
import com.dcp.outlet.adapters.JarLogicLoader;
import com.dcp.outlet.control.LogicCatalog;
import com.dcp.outlet.control.LogicInfo;
import com.dcp.outlet.control.LogicPublishException;
import com.dcp.outlet.sdk.ConsumerLogicDef;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicRef;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 逻辑管理生产化:把"手工五步"收编为一次发布动作(七步),并提供列表 / 版本列表 / 启停 / 删除 / 下载。
 *
 * <p>七步:① 接收+大小/魔数校验 → ② 试加载预检(算哈希+独立 ClassLoader 加载+声明一致)→ ③ 算 sha256
 * → ④ 写仓库(PutObject 原子 / 临时名→sidecar→原子改名, 见 LogicArtifactStore 实现)→ ⑤ 事务登记
 * → ⑥ 审计 → ⑦ 缺哈希 fail-closed(加载端闭环)。
 *
 * <p>M7: 制品仓库抽象为 {@link LogicArtifactStore} 端口——生产走 MinIO 共享桶
 * ({@link MinioArtifactStore}), 单机/测试走本地 ({@link FileArtifactStore})。
 *
 * <p>本类不依赖 Spring;依赖通过构造注入(LogicCatalog 端口 + 制品仓库 + 元库 DataSource 用于引用检查)。
 */
public class LogicService {

    public static final String ACTION_PUBLISH = "publish";
    public static final String ACTION_DISABLE = "disable";
    public static final String ACTION_ENABLE = "enable";
    public static final String ACTION_ARCHIVE = "archive";

    private static final long DEFAULT_MAX_JAR_BYTES = 50L * 1024 * 1024; // 50 MB

    private final LogicCatalog catalog;
    private final DataSource ds;
    private final LogicArtifactStore store;
    private final long maxJarBytes;

    public LogicService(LogicCatalog catalog, DataSource ds, LogicArtifactStore store) {
        this(catalog, ds, store, DEFAULT_MAX_JAR_BYTES);
    }

    public LogicService(LogicCatalog catalog, DataSource ds, LogicArtifactStore store, long maxJarBytes) {
        this.catalog = catalog;
        this.ds = ds;
        this.store = store;
        this.maxJarBytes = maxJarBytes;
    }

    /** 发布结果(成功时携带登记信息)。 */
    public record PublishResult(String logicId, String version, String artifactHash, long jarSize,
                                String status, String preflight) {
    }

    /** 下载制品(jar 字节 + 文件名)。 */
    public record Download(byte[] bytes, String fileName) {
    }

    /**
     * 上传发布一次动作(七步)。
     *
     * @throws LogicPublishException 版本冲突 / 预检失败 / 声明不一致等拒绝路径
     */
    public PublishResult publish(byte[] jar, String logicId, String version, String description, String operator) {
        if (jar == null || jar.length == 0) throw new LogicPublishException("jar 为空");
        if (isBlank(logicId) || isBlank(version)) throw new LogicPublishException("logicId 与 version 必填");

        // ① 大小 + 魔数校验
        if (jar.length > maxJarBytes) {
            throw new LogicPublishException("jar 超过大小限制(" + maxJarBytes + " 字节): " + jar.length);
        }
        checkMagic(jar);

        // 禁止覆盖:写仓库前先判版本冲突(友好提示;最终以 DB 主键 + 无覆盖原子改名兜底)
        if (catalog.find(logicId, version).isPresent()) {
            throw new LogicPublishException("版本已存在,禁止覆盖:" + logicId + "@" + version
                    + "(发错内容请发新版本)");
        }

        // ② 试加载预检(算哈希 + 独立 ClassLoader 加载 + 声明一致)③ 返回 sha256
        String sha256 = preflightAndHash(jar, logicId, version);

        // ④ 写仓库(PutObject 原子 / 临时名→sidecar→原子改名, 见 store 实现)
        store.putArtifact(logicId, version, jar, sha256);

        // ⑤ 事务登记(版本冲突兜底由 register 抛出)→ 失败回滚刚写入的制品
        LogicInfo info = LogicInfo.builder(logicId, version)
                .status(LogicInfo.STATUS_ACTIVE)
                .artifactUri(store.describe(logicId, version))
                .artifactHash(sha256)
                .publishedBy(operator)
                .jarSize((long) jar.length)
                .description(description)
                .preflight("通过: sha256=" + sha256)
                .build();
        try {
            catalog.register(info);
        } catch (LogicPublishException e) {
            store.deleteArtifact(logicId, version);
            throw e;
        }

        // ⑥ 审计
        catalog.audit(logicId, version, ACTION_PUBLISH, operator,
                "sha256=" + sha256 + " size=" + jar.length);

        return new PublishResult(logicId, version, sha256, jar.length,
                info.status(), info.preflight());
    }

    /** 逻辑列表(logicId 聚合 + 最新版本 + 版本数 + 绑定任务数 + 状态徽标)。 */
    public List<Map<String, Object>> list() {
        Map<String, List<LogicInfo>> byId = new LinkedHashMap<>();
        for (LogicInfo li : catalog.all()) {
            byId.computeIfAbsent(li.logicId(), k -> new ArrayList<>()).add(li);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map.Entry<String, List<LogicInfo>> e : byId.entrySet()) {
            String logicId = e.getKey();
            List<LogicInfo> versions = e.getValue();
            // catalog.all() 已按 registered_at DESC 排;取第一条为最新
            LogicInfo latest = versions.get(0);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("logicId", logicId);
            m.put("latestVersion", latest.version());
            m.put("status", latest.status());
            m.put("versionCount", versions.size());
            m.put("boundTasks", countTaskBindings(logicId));
            out.add(m);
        }
        return out;
    }

    /** 版本列表(状态/哈希/大小/发布人/时间/预检结果/绑定任务数)。 */
    public List<Map<String, Object>> versionsOf(String logicId) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (LogicInfo li : catalog.versionsOf(logicId)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("logicId", li.logicId());
            m.put("version", li.version());
            m.put("status", li.status());
            m.put("artifactHash", li.artifactHash());
            m.put("jarSize", li.jarSize());
            m.put("publishedBy", li.publishedBy());
            m.put("registeredAt", li.registeredAt());
            m.put("preflight", li.preflight());
            m.put("description", li.description());
            m.put("boundTasks", countTaskBindings(logicId, li.version()));
            m.put("boundTaskIds", taskBindings(logicId, li.version()));
            out.add(m);
        }
        return out;
    }

    /** 版本级启停(disable/enable)。 */
    public void setVersionStatus(String logicId, String version, String status, String operator) {
        ensureVersion(logicId, version);
        catalog.setStatus(logicId, version, status);
        catalog.audit(logicId, version, status, operator, "version status → " + status);
    }

    /** 逻辑级启停:该 logicId 全部版本统一状态。 */
    public void setLogicStatus(String logicId, String status, String operator) {
        List<LogicInfo> versions = catalog.versionsOf(logicId);
        if (versions.isEmpty()) throw new LogicPublishException("逻辑不存在:" + logicId);
        for (LogicInfo v : versions) {
            catalog.setStatus(logicId, v.version(), status);
            catalog.audit(logicId, v.version(), status, operator, "logic status → " + status);
        }
    }

    /**
     * 删除(= 标记 archived,软删除;被任务引用的版本拒绝)。
     *
     * @return 被引用时返回 {@code false} 并带原因;成功返回 {@code true}
     */
    public Map<String, Object> deleteVersion(String logicId, String version, String operator) {
        ensureVersion(logicId, version);
        List<String> refs = taskBindings(logicId, version);
        if (!refs.isEmpty()) {
            return Map.of("ok", false, "reason", "被 " + refs.size() + " 个任务引用,拒绝删除: " + refs);
        }
        catalog.setStatus(logicId, version, LogicInfo.STATUS_ARCHIVED);
        catalog.audit(logicId, version, ACTION_ARCHIVE, operator, "delete → archived");
        return Map.of("ok", true, "logicId", logicId, "version", version,
                "status", LogicInfo.STATUS_ARCHIVED);
    }

    /** 下载 jar(带审计)。 */
    public Download download(String logicId, String version, String operator) {
        ensureVersion(logicId, version);
        catalog.audit(logicId, version, "download", operator, "download jar");
        return new Download(store.getArtifact(logicId, version), version + ".jar");
    }

    /* ------------------------------------------------ 内部 */

    /** 试加载预检 + 算 sha256。复用 FileLogicRepository + JarLogicLoader 加载链。 */
    private String preflightAndHash(byte[] jar, String logicId, String version) {
        Path preflightRoot = null;
        LoadedLogic loaded = null;
        try {
            preflightRoot = Files.createTempDirectory("logic-preflight");
            Path dir = preflightRoot.resolve(logicId);
            Files.createDirectories(dir);
            Path jarPath = dir.resolve(version + ".jar");
            Files.write(jarPath, jar);
            String sha = FileLogicRepository.sha256(jarPath);
            // 写 sidecar 后加载:既校验哈希,又走真实的独立 ClassLoader + ServiceLoader 发现
            Files.writeString(dir.resolve(version + ".jar.sha256"), sha + "  " + version + ".jar\n");
            loaded = new JarLogicLoader(new FileLogicRepository(preflightRoot))
                    .load(LogicRef.of(logicId, version));
            checkAnnotation(loaded, logicId, version);
            return sha;
        } catch (LogicPublishException e) {
            throw e; // 注解声明不一致等,原样抛出
        } catch (com.dcp.outlet.sdk.LogicNotFoundException e) {
            throw new LogicPublishException("试加载预检失败(缺 SPI 声明或声明不一致):"
                    + logicId + "@" + version + " —— " + e.getMessage(), e);
        } catch (Exception e) {
            // 试加载失败(类加载失败 / 哈希缺失或失配 fail-closed 等)统一拒绝
            throw new LogicPublishException("试加载预检失败:" + logicId + "@" + version + " —— " + e.getMessage(), e);
        } finally {
            if (loaded != null) { try { loaded.close(); } catch (Exception ignored) { } }
            deleteRecursivelyQuietly(preflightRoot);
        }
    }

    /** 注解声明一致性(当前加载链以 SPI ProviderEntry.ref 为权威声明,注解可选;存在则强校验)。 */
    private void checkAnnotation(LoadedLogic loaded, String logicId, String version) {
        ConsumerLogicDef def = loaded.logic().getClass().getAnnotation(ConsumerLogicDef.class);
        if (def == null) return;
        if (!logicId.equals(def.logicId()) || !version.equals(def.version())) {
            throw new LogicPublishException("注解声明不一致: 上传 " + logicId + "@" + version
                    + " 但 @ConsumerLogicDef 声明 " + def.logicId() + "@" + def.version());
        }
    }

    /** jar 魔数(0x504B0304 = PK\\x03\\x04)。 */
    private void checkMagic(byte[] jar) {
        if (jar.length < 4) throw new LogicPublishException("jar 太小,非有效 jar");
        boolean zip = (jar[0] & 0xFF) == 0x50 && (jar[1] & 0xFF) == 0x4B
                && (jar[2] & 0xFF) == 0x03 && (jar[3] & 0xFF) == 0x04;
        if (!zip) throw new LogicPublishException("文件魔数不符,非有效 jar");
    }

    private void ensureVersion(String logicId, String version) {
        if (catalog.find(logicId, version).isEmpty()) {
            throw new LogicPublishException("版本不存在:" + logicId + "@" + version);
        }
    }

    private int countTaskBindings(String logicId) {
        return queryInt("SELECT COUNT(*) FROM consumer_task WHERE logic_id=?", logicId, null);
    }

    private int countTaskBindings(String logicId, String version) {
        return queryInt("SELECT COUNT(*) FROM consumer_task WHERE logic_id=? AND logic_version=?",
                logicId, version);
    }

    private List<String> taskBindings(String logicId, String version) {
        String sql = "SELECT task_id FROM consumer_task WHERE logic_id=? AND logic_version=?";
        List<String> out = new ArrayList<>();
        try (java.sql.Connection c = ds.getConnection();
             java.sql.PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, logicId);
            ps.setString(2, version);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("任务绑定查询失败:" + logicId + "@" + version, e);
        }
        return out;
    }

    private int queryInt(String sql, String a, String b) {
        try (java.sql.Connection c = ds.getConnection();
             java.sql.PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, a);
            if (b != null) ps.setString(2, b);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("计数查询失败", e);
        }
    }

    private static boolean isBlank(String s) { return s == null || s.isBlank(); }

    private static void deleteRecursivelyQuietly(Path p) {
        if (p == null || !Files.exists(p)) return;
        try (var walk = Files.walk(p)) {
            walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(x -> { try { Files.deleteIfExists(x); } catch (IOException ignored) { } });
        } catch (IOException ignored) { }
    }
}

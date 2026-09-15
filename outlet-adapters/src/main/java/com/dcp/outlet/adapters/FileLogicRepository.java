package com.dcp.outlet.adapters;

import com.dcp.outlet.sdk.LogicRef;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Optional;

/**
 * 本地目录版逻辑仓库:约定布局 {@code <root>/<logicId>/<version>.jar},
 * 可选同目录 {@code <version>.jar.sha256}(存在则校验)。
 *
 * <p>与发布流程的对接:注册时把 jar 落到该目录并写哈希即可。
 */
public class FileLogicRepository implements LogicRepository {

    private final Path root;

    public FileLogicRepository(Path root) { this.root = root; }

    @Override
    public Optional<LogicArtifact> locate(LogicRef ref) {
        Path jar = root.resolve(ref.logicId()).resolve(ref.version() + ".jar");
        if (!Files.isRegularFile(jar)) return Optional.empty();
        String sha = readHash(jar);
        return Optional.of(new LogicArtifact(ref.logicId(), ref.version(), jar, sha));
    }

    private String readHash(Path jar) {
        Path side = jar.resolveSibling(jar.getFileName().toString() + ".sha256");
        if (Files.isRegularFile(side)) {
            try {
                String s = Files.readString(side).trim();
                int sp = s.indexOf(' ');
                return (sp > 0 ? s.substring(0, sp) : s).toLowerCase();
            } catch (IOException ignored) {
                return null;
            }
        }
        return null;
    }

    /** 计算 jar 的 sha256(发布流程写 sidecar 时用)。 */
    public static String sha256(Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
            StringBuilder sb = new StringBuilder();
            for (byte b : md.digest()) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("计算 sha256 失败:" + file, e);
        }
    }
}

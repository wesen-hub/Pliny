package com.dcp.outlet.service.logic;

import com.dcp.outlet.control.LogicPublishException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * 本地目录制品仓库(LogicService 原写仓库逻辑迁入): {@code <root>/<logicId>/<version>.jar}。
 *
 * <p>原子性: 临时名 → sidecar 先落地 → jar 最后原子改名(无覆盖), jar 可见 ⟹ sidecar
 * 已可见(加载端 fail-closed 闭环)。
 */
public class FileArtifactStore implements LogicArtifactStore {

    private final Path root;

    public FileArtifactStore(Path root) {
        this.root = root;
    }

    private Path jarPath(String logicId, String version) {
        return root.resolve(logicId).resolve(version + ".jar");
    }

    @Override
    public void putArtifact(String logicId, String version, byte[] jar, String sha256) {
        Path jarPath = jarPath(logicId, version);
        try {
            Path dir = jarPath.getParent();
            Files.createDirectories(dir);
            Path tmpJar = dir.resolve(version + ".jar.tmp-" + UUID.randomUUID());
            Path tmpSide = dir.resolve(version + ".jar.sha256.tmp-" + UUID.randomUUID());
            try {
                Files.write(tmpJar, jar);
                Files.writeString(tmpSide, sha256 + "  " + version + ".jar\n");
                // sidecar 先落地, jar 最后原子改名 → jar 可见 ⟹ sidecar 已可见
                moveNoReplace(tmpSide, dir.resolve(version + ".jar.sha256"));
                moveNoReplace(tmpJar, jarPath);
            } finally {
                deleteQuietly(tmpJar);
                deleteQuietly(tmpSide);
            }
        } catch (IOException e) {
            throw new LogicPublishException("写仓库失败:" + jarPath, e);
        }
    }

    @Override
    public byte[] getArtifact(String logicId, String version) {
        Path jarPath = jarPath(logicId, version);
        if (!Files.isRegularFile(jarPath)) {
            throw new LogicPublishException("jar 不存在于仓库:" + logicId + "@" + version);
        }
        try {
            return Files.readAllBytes(jarPath);
        } catch (IOException e) {
            throw new LogicPublishException("读取 jar 失败:" + jarPath, e);
        }
    }

    @Override
    public void deleteArtifact(String logicId, String version) {
        Path jarPath = jarPath(logicId, version);
        deleteQuietly(jarPath);
        deleteQuietly(jarPath.resolveSibling(version + ".jar.sha256"));
    }

    @Override
    public String describe(String logicId, String version) {
        return "repo:" + jarPath(logicId, version);
    }

    private static void moveNoReplace(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(from, to); // 退化为普通改名(仍无覆盖)
        }
    }

    private static void deleteQuietly(Path p) {
        if (p == null) return;
        try { Files.deleteIfExists(p); } catch (IOException ignored) { }
    }
}

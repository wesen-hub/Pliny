package com.dcp.outlet.adapters;

import java.nio.file.Path;

/** 逻辑制品(jar):位置 + 期望哈希(sha256,大小写不敏感;为空表示不校验)。 */
public final class LogicArtifact {

    private final String logicId;
    private final String version;
    private final Path path;
    private final String sha256;

    public LogicArtifact(String logicId, String version, Path path, String sha256) {
        this.logicId = logicId; this.version = version; this.path = path; this.sha256 = sha256;
    }

    public String logicId() { return logicId; }
    public String version() { return version; }
    public Path path() { return path; }
    public String sha256() { return sha256; }

    @Override public String toString() { return logicId + "@" + version + " → " + path; }
}

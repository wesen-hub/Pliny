package com.dcp.outlet.sdk;

import java.util.Objects;

/** 逻辑引用:{@code logicId@version}。任务的 {@code logic} 字段即此结构。 */
public final class LogicRef {

    private final String logicId;
    private final String version;

    private LogicRef(String logicId, String version) {
        this.logicId = Objects.requireNonNull(logicId, "logicId");
        this.version = Objects.requireNonNull(version, "version");
    }

    public static LogicRef of(String logicId, String version) {
        return new LogicRef(logicId, version);
    }

    public String logicId() { return logicId; }

    public String version() { return version; }

    /** 形如 {@code es-sync@1.0.0},用于日志与错误信息。 */
    @Override
    public String toString() { return logicId + "@" + version; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogicRef)) return false;
        LogicRef r = (LogicRef) o;
        return logicId.equals(r.logicId) && version.equals(r.version);
    }

    @Override
    public int hashCode() { return Objects.hash(logicId, version); }
}

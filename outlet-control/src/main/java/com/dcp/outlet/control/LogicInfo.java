package com.dcp.outlet.control;

/**
 * 逻辑注册项(consumer_logic 一行的领域值对象)。
 *
 * <p>内核纯度:零框架依赖,只承载数据。字段与 {@code consumer_logic} 表对齐:
 * 除任务给定的 8 字段外,补充 {@code description} 与 {@code preflight} 两列,
 * 使"版本详情(试加载预检结果)"与"发布登记(描述)"能够完整映射表行。
 */
public final class LogicInfo {

    /** 状态取值(与 DDL 的 status 列一致)。 */
    public static final String STATUS_ACTIVE = "active";
    public static final String STATUS_DISABLED = "disabled";
    public static final String STATUS_ARCHIVED = "archived";

    private final String logicId;
    private final String version;
    private final String status;
    private final String artifactUri;
    private final String artifactHash;
    private final String publishedBy;
    private final Long jarSize;
    private final String registeredAt;
    private final String description;
    private final String preflight;

    private LogicInfo(Builder b) {
        this.logicId = b.logicId;
        this.version = b.version;
        this.status = b.status == null ? STATUS_ACTIVE : b.status;
        this.artifactUri = b.artifactUri;
        this.artifactHash = b.artifactHash;
        this.publishedBy = b.publishedBy;
        this.jarSize = b.jarSize;
        this.registeredAt = b.registeredAt;
        this.description = b.description;
        this.preflight = b.preflight;
    }

    public String logicId() { return logicId; }
    public String version() { return version; }
    public String status() { return status; }
    public String artifactUri() { return artifactUri; }
    public String artifactHash() { return artifactHash; }
    public String publishedBy() { return publishedBy; }
    public Long jarSize() { return jarSize; }
    public String registeredAt() { return registeredAt; }
    public String description() { return description; }
    public String preflight() { return preflight; }

    /** 该版本是否处于"可被新任务引用"的状态(active)。 */
    public boolean active() { return STATUS_ACTIVE.equals(status); }

    @Override public String toString() { return logicId + "@" + version + "(" + status + ")"; }

    public static Builder builder(String logicId, String version) {
        return new Builder(logicId, version);
    }

    /** 流式构造器(与 TaskRecord 的 builder 风格一致)。 */
    public static final class Builder {
        private final String logicId;
        private final String version;
        private String status;
        private String artifactUri;
        private String artifactHash;
        private String publishedBy;
        private Long jarSize;
        private String registeredAt;
        private String description;
        private String preflight;

        private Builder(String logicId, String version) {
            this.logicId = logicId;
            this.version = version;
        }

        public Builder status(String v) { this.status = v; return this; }
        public Builder artifactUri(String v) { this.artifactUri = v; return this; }
        public Builder artifactHash(String v) { this.artifactHash = v; return this; }
        public Builder publishedBy(String v) { this.publishedBy = v; return this; }
        public Builder jarSize(Long v) { this.jarSize = v; return this; }
        public Builder registeredAt(String v) { this.registeredAt = v; return this; }
        public Builder description(String v) { this.description = v; return this; }
        public Builder preflight(String v) { this.preflight = v; return this; }

        public LogicInfo build() { return new LogicInfo(this); }
    }
}

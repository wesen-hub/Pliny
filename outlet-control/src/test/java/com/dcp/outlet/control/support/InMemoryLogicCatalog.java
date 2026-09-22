package com.dcp.outlet.control.support;

import com.dcp.outlet.control.LogicCatalog;
import com.dcp.outlet.control.LogicInfo;
import com.dcp.outlet.control.LogicPublishException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 内存逻辑目录(测试用):实现生产化扩展后的 {@link LogicCatalog} 全量方法。 */
public class InMemoryLogicCatalog implements LogicCatalog {

    private final Map<String, LogicInfo> byKey = new LinkedHashMap<>();
    private final List<String> auditLog = new ArrayList<>();

    /** 便捷注册(默认 active)。 */
    public InMemoryLogicCatalog register(String logicId, String version) {
        register(LogicInfo.builder(logicId, version).status(LogicInfo.STATUS_ACTIVE).build());
        return this;
    }

    @Override public boolean registered(String logicId, String version) {
        return byKey.containsKey(key(logicId, version));
    }

    @Override public Optional<LogicInfo> find(String logicId, String version) {
        return Optional.ofNullable(byKey.get(key(logicId, version)));
    }

    @Override public List<LogicInfo> versionsOf(String logicId) {
        List<LogicInfo> out = new ArrayList<>();
        for (LogicInfo li : byKey.values()) if (li.logicId().equals(logicId)) out.add(li);
        return out;
    }

    @Override public List<LogicInfo> all() {
        return new ArrayList<>(byKey.values());
    }

    @Override public void register(LogicInfo info) {
        String k = key(info.logicId(), info.version());
        if (byKey.containsKey(k)) {
            throw new LogicPublishException("版本已存在,禁止覆盖:" + info.logicId() + "@" + info.version());
        }
        byKey.put(k, info);
    }

    @Override public void setStatus(String logicId, String version, String status) {
        LogicInfo old = byKey.get(key(logicId, version));
        if (old == null) throw new LogicPublishException("版本不存在:" + logicId + "@" + version);
        byKey.put(key(logicId, version), LogicInfo.builder(logicId, version)
                .status(status)
                .artifactUri(old.artifactUri()).artifactHash(old.artifactHash())
                .publishedBy(old.publishedBy()).jarSize(old.jarSize())
                .registeredAt(old.registeredAt()).description(old.description())
                .preflight(old.preflight()).build());
    }

    @Override public void audit(String logicId, String version, String action, String operator, String detail) {
        auditLog.add(action + ":" + logicId + "@" + version + ":" + operator + ":" + detail);
    }

    public int auditCount() { return auditLog.size(); }

    private static String key(String logicId, String version) { return logicId + "@" + version; }
}

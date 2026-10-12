package com.perm.admin.meta;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 内存目录实现（perm.metadata.source=memory，默认）
 * 目录来自配置（perm.metadata.attributes / tag-paths），注册接口运行时扩充（热生效）。
 * 正式形态替换为 JdbcMetadataService / 企业元数据中心适配器。
 */
@Component
@ConditionalOnProperty(name = "perm.metadata.source", havingValue = "memory", matchIfMissing = true)
public class InMemoryMetadataService implements MetadataService {

    private final CopyOnWriteArrayList<String> attributeRefs;
    private final CopyOnWriteArrayList<String> tagPaths;

    public InMemoryMetadataService(
            @Value("${perm.metadata.attributes:subject.region,subject.allowedStatus,subject.depts,subject.groups}")
            List<String> attributes,
            @Value("${perm.metadata.tag-paths:业务域/客户价值/高价值,业务域/客户价值/中价值,业务域/区域/华东}")
            List<String> tagPaths) {
        this.attributeRefs = new CopyOnWriteArrayList<>(attributes);
        this.tagPaths = new CopyOnWriteArrayList<>(tagPaths);
    }

    @Override
    public List<String> attributeRefs() {
        return List.copyOf(attributeRefs);
    }

    @Override
    public List<String> tagPaths() {
        return List.copyOf(tagPaths);
    }

    @Override
    public void registerAttribute(String ref, String channel, String description) {
        attributeRefs.addIfAbsent(ref);
    }

    @Override
    public void registerTagPath(String path, String channel, String description) {
        tagPaths.addIfAbsent(path);
    }
}

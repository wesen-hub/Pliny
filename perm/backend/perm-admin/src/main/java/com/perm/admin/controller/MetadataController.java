package com.perm.admin.controller;

import com.perm.admin.meta.MetadataService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 元数据目录接口（管理面维护 C7/C8 校验目录）
 * GET  /api/v1/meta/attributes：已知主体属性引用目录
 * GET  /api/v1/meta/tags       ：已知标签路径目录
 * POST /api/v1/meta/attributes：注册新属性引用（幂等）
 * POST /api/v1/meta/tags       ：注册新标签路径（幂等）
 */
@RestController
@RequestMapping("/api/v1/meta")
public class MetadataController {

    public record Entry(String ref, String channel, String description) {
    }

    private final MetadataService metadata;

    public MetadataController(MetadataService metadata) {
        this.metadata = metadata;
    }

    @GetMapping("/attributes")
    public Map<String, Object> attributes() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("attributes", metadata.attributeRefs());
        return body;
    }

    @GetMapping("/tags")
    public Map<String, Object> tags() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("tagPaths", metadata.tagPaths());
        return body;
    }

    @PostMapping("/attributes")
    public Map<String, Object> registerAttribute(@RequestBody Entry entry) {
        metadata.registerAttribute(entry.ref(), entry.channel(), entry.description());
        return attributes();
    }

    @PostMapping("/tags")
    public Map<String, Object> registerTagPath(@RequestBody Entry entry) {
        metadata.registerTagPath(entry.ref(), entry.channel(), entry.description());
        return tags();
    }
}

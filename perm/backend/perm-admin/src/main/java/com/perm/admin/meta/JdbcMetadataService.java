package com.perm.admin.meta;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * JDBC 目录实现（perm.metadata.source=jdbc，随 jdbc profile 启用）
 * 目录持久化于 perm_attr_meta / perm_tag_meta（schema.sql 建表 + 种子），
 * 注册接口幂等（INSERT IGNORE），查询实时读库——目录维护即数据维护。
 */
@Component
@ConditionalOnProperty(name = "perm.metadata.source", havingValue = "jdbc")
public class JdbcMetadataService implements MetadataService {

    private final JdbcTemplate jdbc;

    public JdbcMetadataService(JdbcTemplate jdbcTemplate) {
        this.jdbc = jdbcTemplate;
    }

    @Override
    public List<String> attributeRefs() {
        return jdbc.queryForList("SELECT attr_ref FROM perm_attr_meta ORDER BY attr_ref", String.class);
    }

    @Override
    public List<String> tagPaths() {
        return jdbc.queryForList("SELECT tag_path FROM perm_tag_meta ORDER BY tag_path", String.class);
    }

    @Override
    public void registerAttribute(String ref, String channel, String description) {
        jdbc.update("INSERT IGNORE INTO perm_attr_meta (attr_ref, channel, description) VALUES (?, ?, ?)",
                ref, channel, description);
    }

    @Override
    public void registerTagPath(String path, String channel, String description) {
        jdbc.update("INSERT IGNORE INTO perm_tag_meta (tag_path, channel, description) VALUES (?, ?, ?)",
                path, channel, description);
    }
}

package com.dcp.asset;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 资产目录 + 血缘域独立服务(方案 N12)。
 *
 * <p>承载 backend/module/assetcat(人工业务打标/标签树) 与 module/lineage(Neo4j 血缘图);
 * 打标管理面走用户 JWT + @perm.isAdmin(与 backend 同套安全套件),
 * 目录/血缘读接口与血缘重建也需登录(前端带 token), 仅 actuator permitAll。
 */
@SpringBootApplication
public class AssetServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AssetServiceApplication.class, args);
    }
}

package com.dcp.laketask.lake;

import com.dcp.laketask.LakeParams;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Lake catalog 装配(与 dcp-lakesync Spark catalog 配置同源同键):
 * type=rest → RESTCatalog(Polaris 等 REST 服务端; 推荐 credential+scope+oauth2ServerUri
 *   长驻自动换 token, 或短时 token); type=hadoop → HadoopCatalog(s3 基路径 + S3FileIO)。
 * io-impl/s3.* 与批作业 lake 段完全同构。
 */
public final class LakeCatalogFactory {

    private LakeCatalogFactory() {
    }

    public static Catalog open(LakeParams.Lake lake) {
        Map<String, String> props = new HashMap<>();
        if (!isBlank(lake.uri)) props.put("uri", lake.uri);
        if (!isBlank(lake.warehouse)) props.put("warehouse", lake.warehouse);
        if (!isBlank(lake.token)) props.put("token", lake.token);
        if (!isBlank(lake.credential)) {
            props.put("credential", lake.credential);
            if (!isBlank(lake.scope)) props.put("scope", lake.scope);
            if (!isBlank(lake.oauth2ServerUri)) props.put("oauth2-server-uri", lake.oauth2ServerUri);
            // 与批作业 Spark 配置同款(REST 服务端下发 vended credentials)
            props.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
        }
        if (!isBlank(lake.ioImpl)) props.put("io-impl", lake.ioImpl);
        if (lake.s3 != null) {
            // 与批作业 LakeSyncJob.buildSpark 的 s3 键映射逐一对齐
            Map<String, String> s3Props = Map.of(
                    "endpoint", "s3.endpoint",
                    "pathStyleAccess", "s3.path-style-access",
                    "accessKey", "s3.access-key-id",
                    "secretKey", "s3.secret-access-key",
                    "region", "s3.region");
            s3Props.forEach((k, prop) -> {
                String v = lake.s3.get(k);
                if (v != null) props.put(prop, v);
            });
        }
        Catalog catalog;
        if ("hadoop".equalsIgnoreCase(lake.type)) {
            catalog = CatalogUtil.buildIcebergCatalog(lake.catalog, props, null);
        } else {
            catalog = new RESTCatalog();
            catalog.initialize(lake.catalog, props);
        }
        return catalog;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}

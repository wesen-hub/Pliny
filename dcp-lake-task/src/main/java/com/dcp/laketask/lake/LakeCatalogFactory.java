package com.dcp.laketask.lake;

import com.dcp.laketask.LakeParams;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Lake catalog 装配(与 dcp-lakesync Spark catalog 配置同源同键):
 * type=rest → RESTCatalog(Polaris 等 REST 服务端; 推荐 credential+scope+oauth2ServerUri
 *   长驻自动换 token, 或短时 token); type=hadoop → HadoopCatalog(s3 基路径 + S3FileIO,
 *   metadata 面走 Hadoop fs 需 s3 scheme 注册); type=jdbc → JdbcCatalog(元数据存 RDS,
 *   免 Hadoop fs, 数据与 metadata 文件走 io-impl)。
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
                    // iceberg-aws 1.7.1 起 region 键从 s3.region 迁到 AwsClientProperties.CLIENT_REGION(client.region)
                    "region", "client.region");
            s3Props.forEach((k, prop) -> {
                String v = lake.s3.get(k);
                if (v != null) props.put(prop, v);
            });
        }
        Catalog catalog;
        if ("hadoop".equalsIgnoreCase(lake.type)) {
            // 直接构造(不走 CatalogUtil.buildIcebergCatalog 反射: 湖 jar 由独立 URLClassLoader 加载,
            // 反射用 TCCL 找不到 jar 内 HadoopCatalog → ClassNotFoundException);
            // 无参构造的 conf 未初始化(initialize 内 Util.getFs 会 NPE), 需显式 setConf
            HadoopCatalog hadoop = new HadoopCatalog();
            hadoop.setConf(new org.apache.hadoop.conf.Configuration());
            hadoop.initialize(lake.catalog, props);
            catalog = hadoop;
        } else if ("jdbc".equalsIgnoreCase(lake.type)) {
            // JDBC Catalog: 元数据存 RDS(uri 下发), 表数据与 metadata 文件走 io-impl(S3FileIO);
            // 无需 Hadoop fs/s3 scheme 注册, 三机无 polaris 场景下湖增量的标准路径
            org.apache.iceberg.jdbc.JdbcCatalog jdbc = new org.apache.iceberg.jdbc.JdbcCatalog();
            jdbc.initialize(lake.catalog, props);
            catalog = jdbc;
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

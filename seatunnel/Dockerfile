# SeaTunnel Zeta 2.3.12 自定义镜像（多阶段）
# Stage 1: 构建 AviatorTransform 自定义插件
FROM maven:3.9-eclipse-temurin-17 AS plugin-build
COPY settings-docker.xml /root/.m2/settings.xml
COPY aviator-transform-src /app/plugin
RUN mvn -q -B -f /app/plugin/pom.xml package -DskipTests

# Stage 1b: 构建 CdcSeqTransform 自定义插件（采集端打标: seq + 桶路由 + CdcRecord 封装）
FROM maven:3.9-eclipse-temurin-17 AS plugin-build-seq
COPY settings-docker.xml /root/.m2/settings.xml
COPY cdc-seq-transform-src /app/plugin
RUN mvn -q -B -f /app/plugin/pom.xml package -DskipTests

# Stage 2: SeaTunnel 发行版 + 插件 + JDBC connector + MySQL 驱动
FROM eclipse-temurin:17-jre

# 发行包与驱动已预下载进构建上下文（避免构建期网络抖动）
COPY dist/apache-seatunnel-2.3.12-bin.tar.gz /tmp/st.tar.gz
RUN tar xzf /tmp/st.tar.gz -C /opt --strip-components=1 && rm /tmp/st.tar.gz

# JDBC connector + connector-common（运行时依赖）+ MySQL 驱动 + MongoDB connector（发行包解压到 /opt, connectors 在 /opt/connectors）
# 依赖 jar 约定目录: ${SEATUNNEL_HOME}/plugins/connector-<name>/（AbstractPluginDiscovery.getPluginDependencyJarPaths 扫描）
COPY dist/connector-jdbc-2.3.12.jar /tmp/connector-jdbc-2.3.12.jar
COPY dist/connector-common-2.3.12.jar /tmp/connector-common-2.3.12.jar
COPY dist/connector-mongodb-2.3.12.jar /tmp/connector-mongodb-2.3.12.jar
COPY dist/connector-cdc-mysql-2.3.12.jar /tmp/connector-cdc-mysql-2.3.12.jar
COPY dist/connector-cdc-postgres-2.3.12.jar /tmp/connector-cdc-postgres-2.3.12.jar
COPY dist/connector-cdc-base-2.3.12.jar /tmp/connector-cdc-base-2.3.12.jar
COPY dist/connector-cdc-sqlserver-2.3.12.jar /tmp/connector-cdc-sqlserver-2.3.12.jar
COPY dist/connector-cdc-mongodb-2.3.12.jar /tmp/connector-cdc-mongodb-2.3.12.jar
COPY dist/connector-cdc-oracle-2.3.12.jar /tmp/connector-cdc-oracle-2.3.12.jar
COPY dist/connector-kafka-2.3.12.jar /tmp/connector-kafka-2.3.12.jar
COPY dist/mysql-connector-j-8.3.0.jar /tmp/mysql-connector-j-8.3.0.jar
COPY dist/postgresql-42.7.4.jar /tmp/postgresql-42.7.4.jar
COPY dist/mssql-jdbc-12.6.1.jre11.jar /tmp/mssql-jdbc-12.6.1.jre11.jar
COPY dist/ojdbc8-21.9.0.0.jar /tmp/ojdbc8-21.9.0.0.jar
RUN mkdir -p /opt/connectors/drivers/jdbc /opt/plugins/connector-jdbc \
    && cp /tmp/connector-jdbc-2.3.12.jar /opt/connectors/connector-jdbc-2.3.12.jar \
    && cp /tmp/connector-common-2.3.12.jar /opt/connectors/connector-common-2.3.12.jar \
    && cp /tmp/connector-mongodb-2.3.12.jar /opt/connectors/connector-mongodb-2.3.12.jar \
    && cp /tmp/connector-cdc-mysql-2.3.12.jar /opt/connectors/connector-cdc-mysql-2.3.12.jar \
    && cp /tmp/connector-cdc-postgres-2.3.12.jar /opt/connectors/connector-cdc-postgres-2.3.12.jar \
    && cp /tmp/connector-cdc-base-2.3.12.jar /opt/connectors/connector-cdc-base-2.3.12.jar \
    && cp /tmp/connector-cdc-sqlserver-2.3.12.jar /opt/connectors/connector-cdc-sqlserver-2.3.12.jar \
    && cp /tmp/connector-cdc-mongodb-2.3.12.jar /opt/connectors/connector-cdc-mongodb-2.3.12.jar \
    && cp /tmp/connector-cdc-oracle-2.3.12.jar /opt/connectors/connector-cdc-oracle-2.3.12.jar \
    && cp /tmp/connector-kafka-2.3.12.jar /opt/connectors/connector-kafka-2.3.12.jar \
    && cp /tmp/mysql-connector-j-8.3.0.jar /opt/connectors/drivers/jdbc/mysql-connector-j-8.3.0.jar \
    && cp /tmp/mysql-connector-j-8.3.0.jar /opt/plugins/connector-jdbc/mysql-connector-j-8.3.0.jar \
    && cp /tmp/postgresql-42.7.4.jar /opt/connectors/drivers/jdbc/postgresql-42.7.4.jar \
    && cp /tmp/postgresql-42.7.4.jar /opt/plugins/connector-jdbc/postgresql-42.7.4.jar \
    && cp /tmp/mssql-jdbc-12.6.1.jre11.jar /opt/connectors/drivers/jdbc/mssql-jdbc-12.6.1.jre11.jar \
    && cp /tmp/mssql-jdbc-12.6.1.jre11.jar /opt/plugins/connector-jdbc/mssql-jdbc-12.6.1.jre11.jar \
    && cp /tmp/ojdbc8-21.9.0.0.jar /opt/connectors/drivers/jdbc/ojdbc8-21.9.0.0.jar \
    && cp /tmp/ojdbc8-21.9.0.0.jar /opt/plugins/connector-jdbc/ojdbc8-21.9.0.0.jar \
    && rm /tmp/connector-jdbc-2.3.12.jar /tmp/connector-common-2.3.12.jar /tmp/connector-mongodb-2.3.12.jar /tmp/connector-cdc-mysql-2.3.12.jar /tmp/connector-cdc-postgres-2.3.12.jar /tmp/connector-cdc-sqlserver-2.3.12.jar /tmp/connector-cdc-mongodb-2.3.12.jar /tmp/connector-cdc-oracle-2.3.12.jar /tmp/connector-kafka-2.3.12.jar /tmp/mysql-connector-j-8.3.0.jar

# 各 CDC connector 目录放全部 JDBC 驱动: connector jar 的 SPI 文件含多个 catalog 工厂
# (如 pg jar 里也有 OracleCatalogFactory), ServiceLoader 遍历时缺任一驱动类就中断,
# 导致排在后面的本连接器 catalog(PostgresCatalogFactory)永远找不到 → 全部驱动放每个目录
RUN mkdir -p /opt/plugins/connector-cdc-mysql /opt/plugins/connector-cdc-postgres /opt/plugins/connector-cdc-sqlserver /opt/plugins/connector-cdc-oracle \
    && for d in connector-cdc-mysql connector-cdc-postgres connector-cdc-sqlserver connector-cdc-oracle; do \
         cp /opt/connectors/drivers/jdbc/mysql-connector-j-8.3.0.jar /opt/plugins/$d/; \
         cp /opt/connectors/drivers/jdbc/postgresql-42.7.4.jar /opt/plugins/$d/; \
         cp /opt/connectors/drivers/jdbc/mssql-jdbc-12.6.1.jre11.jar /opt/plugins/$d/; \
         cp /opt/connectors/drivers/jdbc/ojdbc8-21.9.0.0.jar /opt/plugins/$d/; \
       done

# 自定义插件 + 映射（插件映射已含于发行包 plugin-mapping.properties, 追加自定义项）
# key 格式: seatunnel.transform.<PluginIdentifier>; value 为 jar 文件名前缀（不含 .jar）
COPY --from=plugin-build /app/plugin/target/aviator-transform.jar /opt/connectors/aviator-transform.jar
COPY --from=plugin-build-seq /app/plugin/target/cdc-seq-transform.jar /opt/connectors/cdc-seq-transform.jar
# CdcSeqTransform 需要 JDBC 访问元库领号: 依赖目录名必须与 mapping value 一致(equalsIgnoreCase 匹配),
# 目录名带 connector- 前缀会不匹配而被跳过 → 目录直接叫 cdc-seq-transform(非 connector- 开头, 全扫)
RUN mkdir -p /opt/plugins/cdc-seq-transform \
    && cp /opt/connectors/drivers/jdbc/mysql-connector-j-8.3.0.jar /opt/plugins/cdc-seq-transform/
RUN echo "seatunnel.transform.AviatorTransform=aviator-transform" \
    >> /opt/connectors/plugin-mapping.properties \
    && echo "seatunnel.transform.CdcSeqTransform=cdc-seq-transform" \
    >> /opt/connectors/plugin-mapping.properties

# 引擎配置（REST 8081 / Hazelcast 5801, 覆盖发行包默认配置）
COPY config/seatunnel.yaml /opt/config/seatunnel.yaml
COPY config/hazelcast.yaml /opt/config/hazelcast.yaml
# checkpoint 持久化目录(命名卷 seatunnel-checkpoint 挂载点; 需先存在且可写)
RUN mkdir -p /opt/checkpoint

ENV TZ=Asia/Shanghai
WORKDIR /opt
EXPOSE 8081 5801 5802
ENTRYPOINT ["/opt/bin/seatunnel-cluster.sh"]

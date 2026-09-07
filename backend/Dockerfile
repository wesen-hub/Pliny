# 数据采集平台后端镜像（多阶段构建; .m2 用 buildkit cache mount, 依赖跨构建复用）
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY settings-docker.xml /root/.m2/settings.xml
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -DskipTests dependency:go-offline || true
COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository mvn -B package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
ENV TZ=Asia/Shanghai
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
# Hadoop 客户端(DistCp 引擎; 自带 s3a; 预下载进 build context 避免构建期网络抖动)
COPY dist/hadoop-3.4.1.tar.gz /tmp/hadoop.tar.gz
RUN tar xzf /tmp/hadoop.tar.gz -C /opt && mv /opt/hadoop-3.4.1 /opt/hadoop && rm /tmp/hadoop.tar.gz
# mc 客户端(文件同步新引擎; 与 hadoop 并存供后续切换验证)
RUN curl -sfL -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-arm64/mc && chmod +x /usr/local/bin/mc
COPY --from=build /app/target/dcp.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-Xmx1g", "-jar", "app.jar"]

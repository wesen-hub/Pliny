# 数据采集平台后端镜像（多阶段构建; .m2 用 buildkit cache mount, 依赖跨构建复用）
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY settings-docker.xml /root/.m2/settings.xml
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -DskipTests dependency:go-offline || true
# 内部库 dcp-tag-kie(未发布到远端仓库): 构建前 mvn -f ../dcp-tag-kie/pom.xml package 产出 dist/ 预置 jar,
# 此处 install-file 进本地仓库(带 cache mount, 跨构建复用)
COPY dist/dcp-tag-kie-1.0.0.jar /tmp/dcp-tag-kie-1.0.0.jar
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-tag-kie-1.0.0.jar -DgroupId=com.dcp -DartifactId=dcp-tag-kie -Dversion=1.0.0 -Dpackaging=jar
# 内部库 dcp-scheduler-api(统一调度框架契约, 未发布到远端仓库): 构建前 scripts/build_outlet_dist.sh 产出 dist/ 预置 jar
# 先装 parent pom(dcp-scheduler-parent), 否则解析 api 的 <parent> 会去远端仓库拉取失败
COPY dist/dcp-scheduler-parent-pom.xml /tmp/dcp-scheduler-parent-pom.xml
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-scheduler-parent-pom.xml -DgroupId=com.dcp -DartifactId=dcp-scheduler-parent -Dversion=0.1.0 -Dpackaging=pom
COPY dist/dcp-scheduler-api-0.1.0.jar /tmp/dcp-scheduler-api-0.1.0.jar
COPY dist/dcp-scheduler-api-pom.xml /tmp/dcp-scheduler-api-pom.xml
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-scheduler-api-0.1.0.jar -DpomFile=/tmp/dcp-scheduler-api-pom.xml
COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository mvn -B package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
ENV TZ=Asia/Shanghai
# 注: 文件同步引擎已切 rclone(DistCp/hadoop 客户端已随引擎退役移除, 2026-09-30)
# rclone(文件同步引擎, v1.71.0 arm64; dist/ 预置二进制, 免构建期网络)
COPY dist/rclone /usr/local/bin/rclone
RUN chmod +x /usr/local/bin/rclone
COPY --from=build /app/target/dcp.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-Xmx1g", "-jar", "app.jar"]

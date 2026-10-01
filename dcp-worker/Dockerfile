# dcp-worker 执行体镜像(多阶段构建; .m2 用 buildkit cache mount, 依赖跨构建复用)
# 内部库(dcp-scheduler-api/core, 未发布远端仓库):
#   构建前执行 scripts/build_outlet_dist.sh 产出 dist/ 预置 jar, 此处 install-file 进本地仓库
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY settings-docker.xml /root/.m2/settings.xml
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -DskipTests dependency:go-offline || true
COPY dist/dcp-scheduler-api-0.1.0.jar /tmp/
COPY dist/dcp-scheduler-core-0.1.0.jar /tmp/
COPY dist/dcp-scheduler-api-pom.xml /tmp/
COPY dist/dcp-scheduler-core-pom.xml /tmp/
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-scheduler-api-0.1.0.jar -DpomFile=/tmp/dcp-scheduler-api-pom.xml
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-scheduler-core-0.1.0.jar -DpomFile=/tmp/dcp-scheduler-core-pom.xml
COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository mvn -B package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
ENV TZ=Asia/Shanghai
# rclone(filesync 池起 rclone 子进程; dist/ 预置二进制, 免构建期网络; 与 backend 镜像同源)
COPY dist/rclone /usr/local/bin/rclone
RUN chmod +x /usr/local/bin/rclone
COPY --from=build /app/target/dcp-worker.jar app.jar
EXPOSE 8097
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-Xmx1g", "-jar", "app.jar"]

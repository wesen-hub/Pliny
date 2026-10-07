# outlet 控制面镜像(多阶段构建; .m2 用 buildkit cache mount, 依赖跨构建复用)
# 内部库(dcp-outlet-sdk/control/adapters + dcp-scheduler, 未发布远端仓库):
#   构建前执行 scripts/build_outlet_dist.sh 产出 dist/ 预置 jar, 此处 install-file 进本地仓库
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY settings-docker.xml /root/.m2/settings.xml
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -DskipTests dependency:go-offline || true
COPY dist/dcp-outlet-sdk-2.0.0-SNAPSHOT.jar /tmp/
COPY dist/dcp-outlet-control-2.0.0-SNAPSHOT.jar /tmp/
COPY dist/dcp-outlet-worker-2.0.0-SNAPSHOT.jar /tmp/
COPY dist/dcp-outlet-adapters-2.0.0-SNAPSHOT.jar /tmp/
COPY dist/dcp-scheduler-api-0.1.0.jar /tmp/
# parent pom 先装(dcp-scheduler-parent), 否则 api pom 的 <parent> 解析失败
COPY dist/dcp-scheduler-parent-pom.xml /tmp/
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-scheduler-parent-pom.xml -DgroupId=com.dcp -DartifactId=dcp-scheduler-parent -Dversion=0.1.0 -Dpackaging=pom
COPY dist/dcp-outlet-sdk-pom.xml /tmp/
COPY dist/dcp-outlet-control-pom.xml /tmp/
COPY dist/dcp-outlet-worker-pom.xml /tmp/
COPY dist/dcp-outlet-adapters-pom.xml /tmp/
COPY dist/dcp-scheduler-api-pom.xml /tmp/
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-outlet-sdk-2.0.0-SNAPSHOT.jar -DpomFile=/tmp/dcp-outlet-sdk-pom.xml -DgroupId=com.dcp -DartifactId=dcp-outlet-sdk -Dversion=2.0.0-SNAPSHOT -Dpackaging=jar
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-outlet-control-2.0.0-SNAPSHOT.jar -DpomFile=/tmp/dcp-outlet-control-pom.xml -DgroupId=com.dcp -DartifactId=dcp-outlet-control -Dversion=2.0.0-SNAPSHOT -Dpackaging=jar
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-outlet-worker-2.0.0-SNAPSHOT.jar -DpomFile=/tmp/dcp-outlet-worker-pom.xml -DgroupId=com.dcp -DartifactId=dcp-outlet-worker -Dversion=2.0.0-SNAPSHOT -Dpackaging=jar
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-outlet-adapters-2.0.0-SNAPSHOT.jar -DpomFile=/tmp/dcp-outlet-adapters-pom.xml -DgroupId=com.dcp -DartifactId=dcp-outlet-adapters -Dversion=2.0.0-SNAPSHOT -Dpackaging=jar
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -q org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile=/tmp/dcp-scheduler-api-0.1.0.jar -DpomFile=/tmp/dcp-scheduler-api-pom.xml -DgroupId=com.dcp -DartifactId=dcp-scheduler-api -Dversion=0.1.0 -Dpackaging=jar
COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository mvn -B package -DskipTests -Dstyle.color=never

FROM eclipse-temurin:17-jre
WORKDIR /app
ENV TZ=Asia/Shanghai
COPY --from=build /app/target/dcp-outlet-control-service-2.0.0-SNAPSHOT.jar app.jar
EXPOSE 8095
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-Xmx1g", "-jar", "app.jar"]

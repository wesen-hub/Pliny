# tagengine-executor 镜像: 打标引擎 P0 批执行器(编译验证; E2E 需本地 dev Spark+Iceberg 组合, 后续另排)
# 多阶段构建, 参照 backend/Dockerfile 的 cache mount 写法(.m2 跨构建复用)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY settings-docker.xml /root/.m2/settings.xml
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -DskipTests dependency:go-offline || true
COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository mvn -B package -DskipTests
# 引擎核心自检(无 Spark): 动态 Fact 编译 + DRL 生成 + KieBase 编译 + 会话求值/动作约定 —— 证明 drools 栈在 shade jar 内可用
RUN java -cp /app/target/tagengine-executor.jar com.dcp.tagjob.SmokeMain

# 运行镜像(占位: 作业实际以 spark-submit 形态跑在 dev/集群, 这里仅保证产物可执行环境)
FROM eclipse-temurin:17-jre
WORKDIR /app
ENV TZ=Asia/Shanghai
COPY --from=build /app/target/tagengine-executor.jar app.jar
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-Xmx512m", "-jar", "app.jar"]

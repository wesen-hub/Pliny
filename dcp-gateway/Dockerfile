# 微服务治理网关镜像(阶段 A; 多阶段构建, .m2 用 buildkit cache mount 复用)
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
COPY --from=build /app/target/dcp-gateway-1.0.0.jar app.jar
EXPOSE 8088
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-Xmx512m", "-jar", "app.jar"]

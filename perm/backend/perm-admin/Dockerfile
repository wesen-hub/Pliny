# 策略管理面镜像：宿主机 mvn package 打 jar 后 COPY 进 JDK 镜像
# 基础镜像用本机已有 ubuntu_jdk19（JDK 19 兼容 Java 17 字节码）；
# 网络可达时可用 eclipse-temurin:17-jre 替换（阿里云 mirror 对部分 blob 返回 403）
FROM ubuntu_jdk19:22.04
WORKDIR /app
COPY target/perm-admin-*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]

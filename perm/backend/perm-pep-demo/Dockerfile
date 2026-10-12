# PEP 业务接入示例镜像：宿主机 mvn package 打 jar 后 COPY 进 JDK 镜像
# （与 perm-pdp/perm-admin 同形态，容器内不联网构建）
FROM ubuntu_jdk19:22.04
WORKDIR /app
COPY target/perm-pep-demo-*.jar app.jar
EXPOSE 18083
ENTRYPOINT ["java", "-jar", "app.jar"]

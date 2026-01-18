# 第一阶段：使用 Maven 构建应用
FROM maven:3.8.6-openjdk-17-slim AS builder

# 设置工作目录
WORKDIR /build

# 将 pom.xml 和 src 复制到容器中（利用 Docker 缓存层）
COPY pom.xml .
COPY src ./src

# 构建 JAR 包 (跳过测试以加快构建速度)
RUN mvn clean package -DskipTests

# 第二阶段：运行应用
FROM eclipse-temurin:17-jre-alpine

# 安装必要的工具（可选，如 bash， curl 用于调试）
RUN apk add --no-cache bash curl

# 设置工作目录
WORKDIR /app

# 从构建阶段复制 JAR 包并重命名
COPY --from=builder /build/target/*.jar kohaku-manager.jar

# 创建非 root 用户以增强安全性
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# 暴露端口
EXPOSE 8080

# 启动命令
ENTRYPOINT ["java", "-jar", "kohaku-manager.jar"]

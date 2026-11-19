# km-data-sink-mcp 容器镜像
# 构建上下文要求：本目录下已有 km-data-sink-mcp.jar 与 config.yaml
FROM eclipse-temurin:17-jre

WORKDIR /app

# 时区（日志与 system.now 时间戳保持上海时区，与主 compose 服务一致）
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 产物与配置由 compose 挂载注入（便于升级 jar 不重建镜像），镜像内只放默认副本
COPY km-data-sink-mcp.jar /app/km-data-sink-mcp.jar
COPY config.yaml /app/config.yaml

# 日志目录（容器内路径，由 compose 挂载持久化）
RUN mkdir -p /app/logs

# MCP stdio 纪律：日志走 stderr 与文件；前台常驻等待 stdio 输入
ENTRYPOINT ["java", "-jar", "/app/km-data-sink-mcp.jar", "--config", "/app/config.yaml"]

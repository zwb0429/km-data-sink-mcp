package com.example.datasink.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * YAML 配置加载（snake_case → camelCase：upsert_key → upsertKey 等）。
 */
public class ConfigLoader {

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public AppConfig load(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("配置文件不存在: " + path.toAbsolutePath());
        }
        AppConfig config = mapper.readValue(path.toFile(), AppConfig.class);
        // 补充连接名（Map key）
        config.getConnections().forEach((name, conn) -> {
            if (conn.getName() == null || conn.getName().isBlank()) {
                conn.setName(name);
            }
        });
        return config;
    }
}

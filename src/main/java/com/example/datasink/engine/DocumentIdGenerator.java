package com.example.datasink.engine;

/**
 * document_id 生成（P0-④）：服务端生成 UUID（VARCHAR(36)）。
 * 不依赖业务表主键、不使用业务唯一键拼接。
 */
public final class DocumentIdGenerator {

    private DocumentIdGenerator() {
    }

    public static String generate() {
        return java.util.UUID.randomUUID().toString();
    }
}

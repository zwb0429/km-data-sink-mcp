package com.example.datasink.engine;

import java.util.Locale;

/**
 * 统一错误码（P2-⑮）。
 */
public enum ErrorCode {
    SUCCESS("操作成功"),
    INVALID_ARGUMENT("参数非法"),
    INVALID_CONNECTION("连接名非法"),
    INVALID_TABLE("表名非法"),
    INVALID_COLUMN("列名非法"),
    INVALID_QUERY_CONDITION("查询条件不完整"),
    NON_UNIQUE_RESULT("查询结果不唯一"),
    UNIQUE_KEY_CONFLICT("唯一键冲突（拒绝）"),
    CONTENT_TOO_LARGE("内容超限"),
    DOCUMENT_NOT_FOUND("文档不存在"),
    EXECUTION_ALREADY_RUNNING("执行记录已处于运行中"),
    EXECUTION_ALREADY_SUCCESS("执行记录已成功"),
    EXECUTION_NOT_FOUND("执行记录不存在"),
    DATABASE_CONNECTION_ERROR("数据库连接错误"),
    DATABASE_TRANSACTION_ERROR("事务错误"),
    DATABASE_LOCK_TIMEOUT("锁/超时"),
    SCHEMA_VALIDATION_FAILED("结构校验失败"),
    INTERNAL_ERROR("内部错误");

    private final String defaultMessage;

    ErrorCode(String defaultMessage) {
        this.defaultMessage = defaultMessage;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }

    public String code() {
        return name();
    }

    public static ErrorCode of(String name) {
        try {
            return ErrorCode.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return INTERNAL_ERROR;
        }
    }
}

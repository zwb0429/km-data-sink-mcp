package com.example.datasink.engine;

/**
 * 业务异常：携带统一错误码，工具层捕获后转为 isError(true) 的 CallToolResult。
 * 不得将数据库内部异常堆栈直接返回给客户端，堆栈只写日志。
 */
public class DataSinkException extends RuntimeException {

    private final ErrorCode code;

    public DataSinkException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public DataSinkException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}

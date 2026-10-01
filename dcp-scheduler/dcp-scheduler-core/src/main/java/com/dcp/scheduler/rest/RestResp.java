package com.dcp.scheduler.rest;

/**
 * rest 管理 API 统一响应体(与 backend ApiResponse 同构:
 * {"code":0,"message":"ok","data":...,"traceId":...}),前端无感迁移。
 */
public class RestResp<T> {

    public int code;
    public String message;
    public T data;
    public String traceId;

    public static <T> RestResp<T> ok(T data) {
        RestResp<T> r = new RestResp<>();
        r.code = 0;
        r.message = "ok";
        r.data = data;
        return r;
    }

    public static RestResp<Void> ok() {
        return ok(null);
    }

    public static <T> RestResp<T> error(int code, String message) {
        RestResp<T> r = new RestResp<>();
        r.code = code;
        r.message = message;
        return r;
    }
}

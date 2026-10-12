package com.perm.pep;

/**
 * 判定拒绝/降级异常（fail-closed 语义）
 * 任何异常（PDP 不可达、验签失败、防重放超窗、响应异常）都落到本异常，业务侧一律拒绝执行。
 */
public class PepDeniedException extends RuntimeException {

    public PepDeniedException(String message) {
        super(message);
    }

    public PepDeniedException(String message, Throwable cause) {
        super(message, cause);
    }
}

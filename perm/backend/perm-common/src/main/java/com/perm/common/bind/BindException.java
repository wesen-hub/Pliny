package com.perm.common.bind;

/** 绑定失败：引用无法解析 → 判定降为 DENY（fail-closed，R-F1） */
public class BindException extends RuntimeException {

    private final String ref;

    public BindException(String ref, String message) {
        super(message);
        this.ref = ref;
    }

    public String ref() {
        return ref;
    }
}

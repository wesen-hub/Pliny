package com.perm.common.render;

/** 渲染异常：未知节点/非法节点 → 翻译失败（触发 R-R1 residual 兜底或拒绝） */
public class RenderException extends RuntimeException {

    public RenderException(String message) {
        super(message);
    }
}

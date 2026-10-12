package com.perm.common.model;

/**
 * 谓词值：引用（绑定阶段解析）或字面量
 */
public sealed interface Value permits Value.Ref, Value.Lit {

    /** 主体引用（如 subject.region）——渲染期只产占位符，值在 PEP 绑定阶段出现（铁律 C1） */
    record Ref(String ref) implements Value {
    }

    /** 字面量（渲染期直接绑定为参数，不进 SQL 文本，R-Sec2） */
    record Lit(Object value) implements Value {
    }
}

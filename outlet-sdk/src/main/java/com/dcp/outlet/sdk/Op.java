package com.dcp.outlet.sdk;

/** 单条记录的操作类型。{@link #SNAPSHOT} 仅出现在批阶段。 */
public enum Op {
    /** 快照段读到的行(无 before) */
    SNAPSHOT,
    INSERT,
    UPDATE,
    DELETE
}

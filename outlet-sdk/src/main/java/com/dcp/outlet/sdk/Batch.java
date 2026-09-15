package com.dcp.outlet.sdk;

import java.util.List;

/** 一批待处理记录。已由平台按契约裁剪(列白名单 / 行条件 / 密级 / 脱敏)。 */
public interface Batch {

    Mode mode();

    /** 逻辑表名(如 {@code rt_sync_c}),<b>不是</b>物理 topic。 */
    String stream();

    List<ChangeRecord> records();

    int size();

    boolean isEmpty();
}

package com.perm.admin.publisher;

import java.util.List;

/**
 * 发布流水存档抽象（内存/MySQL 双实现，配置 perm.storage=memory|jdbc 切换）
 * R-M3 第六步确认存档：发布成功后落发布流水表（R-M4 版本单调递增）。
 */
public interface PublicationHistory {

    void add(Publication publication);

    List<Publication> all();
}

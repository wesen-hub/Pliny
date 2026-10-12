package com.perm.admin.publisher;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 内存发布流水（演示/单机模式；perm.storage=memory 时生效，默认）
 */
@Component
@ConditionalOnProperty(name = "perm.storage", havingValue = "memory", matchIfMissing = true)
public class InMemoryPublicationHistory implements PublicationHistory {

    private final List<Publication> history = new CopyOnWriteArrayList<>();

    @Override
    public void add(Publication publication) {
        history.add(publication);
    }

    @Override
    public List<Publication> all() {
        return List.copyOf(history);
    }
}

package com.dcp.outlet.control.support;

import com.dcp.outlet.control.TaskRecord;
import com.dcp.outlet.control.TaskStore;

import java.util.LinkedHashMap;
import java.util.Map;

public class InMemoryTaskStore implements TaskStore {

    private final Map<String, TaskRecord> byId = new LinkedHashMap<>();

    @Override public boolean exists(String taskId) { return byId.containsKey(taskId); }

    @Override public boolean groupExists(String consumerGroup) {
        return byId.values().stream().anyMatch(t -> consumerGroup != null && consumerGroup.equals(t.consumerGroup()));
    }

    @Override public void insert(TaskRecord task) { byId.put(task.taskId(), task); }

    @Override public TaskRecord byId(String taskId) { return byId.get(taskId); }

    @Override public void update(TaskRecord task) { byId.put(task.taskId(), task); }

    public int size() { return byId.size(); }
}

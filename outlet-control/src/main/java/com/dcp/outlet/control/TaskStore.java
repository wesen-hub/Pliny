package com.dcp.outlet.control;

/** 任务存储端口(表 {@code consumer_task})。 */
public interface TaskStore {

    boolean exists(String taskId);

    /** 消费组是否已被占用(一任务一组)。 */
    boolean groupExists(String consumerGroup);

    void insert(TaskRecord task);

    TaskRecord byId(String taskId);

    void update(TaskRecord task);
}

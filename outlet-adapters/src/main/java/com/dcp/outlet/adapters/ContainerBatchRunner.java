package com.dcp.outlet.adapters;

import com.dcp.outlet.control.BatchRunner;
import com.dcp.outlet.control.ContractLookup;
import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;
import com.dcp.outlet.control.T0Selector;
import com.dcp.outlet.control.TaskRecord;
import com.dcp.outlet.control.TaskStore;
import com.dcp.outlet.sdk.SdkLogger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 批容器执行体:{@code BatchRunner} 实现 —— 把打标批阶段提交给独立容器执行。
 *
 * <p>与 {@link SparkBatchRunner} 同构(取任务 → 契约身份 → T0 → task-json → 提交 → 等退出码 → 回调),
 * 载体不同:task-json 挂载进批执行器容器,由容器内嵌的 tagengine-executor 运行。
 *
 * <p>提交方式为 ProcessBuilder 调 docker CLI(与 spark-submit 先例一致,零 SDK 依赖,
 * 避免 docker-java 旧版 jackson 传递依赖与 Boot 3 的运行时冲突);参数全配置化:
 * <pre>
 *   outlet.batch.container.image       批执行器镜像(内置 tagengine-executor fat jar;必配)
 *   outlet.batch.container.cmd         容器启动命令(默认 java -jar /app/tagengine-executor.jar --task-json /mnt/task.json)
 *   outlet.batch.container.mount-dir   宿主机 task-json 落盘目录(默认系统临时目录)
 *   outlet.batch.container.memory      容器内存上限(可选,空不设)
 *   outlet.batch.container.cpus        容器 CPU 上限(可选,空不设)
 * </pre>
 * 未配置 image → 只落 task-json 不提交(任务置 BATCH_FAILED,与 Spark 缺 executor-jar 同口径)。
 */
public class ContainerBatchRunner implements BatchRunner {

    private final TaskStore tasks;
    private final StreamRegistry streams;
    private final T0Selector t0;
    private final SdkLogger log;
    private final ContractLookup contracts;
    /** task-json 组装参数(复用 SparkBatchRunner.buildTaskJson 契约) */
    private final String snapshotBaseUrl;
    private final int epoch;
    private final String catalog;
    private final String namespace;
    private final String warehouse;
    private final String catalogType;
    /** 容器执行参数 */
    private final String image;
    private final String containerCmd;
    private final Path mountDir;
    private final String memory;
    private final String cpus;

    public ContainerBatchRunner(TaskStore tasks, StreamRegistry streams, T0Selector t0, SdkLogger log,
                                ContractLookup contracts, String snapshotBaseUrl, int epoch,
                                String catalog, String namespace, String warehouse, String catalogType,
                                String image, String containerCmd, String mountDir, String memory, String cpus) {
        this.tasks = tasks;
        this.streams = streams;
        this.t0 = t0;
        this.log = log;
        this.contracts = contracts;
        this.snapshotBaseUrl = snapshotBaseUrl;
        this.epoch = epoch;
        this.catalog = catalog;
        this.namespace = namespace;
        this.warehouse = warehouse;
        this.catalogType = catalogType;
        this.image = image;
        this.containerCmd = containerCmd;
        this.mountDir = mountDir == null || mountDir.isBlank()
                ? Path.of(System.getProperty("java.io.tmpdir")) : Path.of(mountDir);
        this.memory = memory;
        this.cpus = cpus;
    }

    @Override
    public void runAsync(String taskId, BatchCallback callback) {
        Thread t = new Thread(() -> {
            try {
                TaskRecord task = tasks.byId(taskId);
                if (task == null || task.streams().isEmpty()) {
                    callback.done(0L, false);
                    return;
                }
                Optional<StreamInfo> si = streams.get(task.streams().get(0));
                if (si.isEmpty()) {
                    callback.done(0L, false);
                    return;
                }
                java.util.Optional<String> identity = SchedulerBatchRunner.resolveConsumer(task, contracts);
                if (identity.isEmpty()) {
                    log.error("容器批阶段失败:任务契约身份不可用(contractRef={}) —— 契约缺失/未生效/无 consumer_id",
                            task.contractRef());
                    callback.done(0L, false);
                    return;
                }
                String consumerId = identity.get();
                T0Selector.Decision d = t0.select(si.get().topic(), 0, null);
                String json = SparkBatchRunner.buildTaskJson(task, si.get().stream(), si.get().topic(),
                        d.tso(), epoch, catalog, namespace, snapshotBaseUrl, consumerId,
                        warehouse, catalogType);
                log.info("容器批阶段 task={} T0={} taskJson={}", taskId, d.tso(), json);

                if (image == null || image.isBlank()) {
                    log.warn("未配置 outlet.batch.container.image → 只落 task-json,不提交容器(任务置 BATCH_FAILED)");
                    callback.done(d.tso(), false);
                } else {
                    boolean ok = submit(json);
                    callback.done(d.tso(), ok);
                }
            } catch (Throwable e) {
                log.error("容器批阶段异常 task=" + taskId, e);
                callback.done(0L, false);
            }
        }, "outlet-container-" + taskId);
        t.setDaemon(true);
        t.start();
    }

    /** 落 task-json → docker run(挂载 + 资源上限)→ 等退出码 */
    private boolean submit(String taskJson) {
        try {
            Files.createDirectories(mountDir);
            Path tmp = Files.createTempFile(mountDir, "dcp-tag-task-", ".json");
            Files.writeString(tmp, taskJson, java.nio.charset.StandardCharsets.UTF_8);
            List<String> cmd = new ArrayList<>(List.of("docker", "run", "--rm",
                    "-v", tmp.toAbsolutePath() + ":/mnt/task.json:ro"));
            if (memory != null && !memory.isBlank()) {
                cmd.add("--memory");
                cmd.add(memory);
            }
            if (cpus != null && !cpus.isBlank()) {
                cmd.add("--cpus");
                cmd.add(cpus);
            }
            cmd.add(image);
            // 容器启动命令(默认 java -jar 内置执行器,读挂载的 task-json)
            cmd.addAll(splitCmd(containerCmd));
            log.info("docker run: {}", String.join(" ", cmd));
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            int exit = p.waitFor();
            log.info("docker run 退出码={} taskJson={}", exit, tmp);
            return exit == 0;
        } catch (Exception e) {
            log.error("docker run 失败", e);
            return false;
        }
    }

    /** 启动命令按空白拆分(命令本身无引号场景;复杂命令建议配置成镜像默认 ENTRYPOINT,cmd 留空) */
    private static List<String> splitCmd(String cmd) {
        if (cmd == null || cmd.isBlank()) {
            return List.of();
        }
        return List.of(cmd.trim().split("\\s+"));
    }
}

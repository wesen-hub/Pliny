package com.dcp.module.outlet.batch;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/** Spark 作业提交器 SPI(设计 §6.3 的可替换点:生产=yarn/k8s 集群,骨架=dry-run/本地进程)。 */
public interface SparkLauncher {

    void launch(SparkJobSpec spec);

    /** dry-run:只记录规格、不真提交(无 Spark 环境时的安全默认,也用于单测) */
    @Slf4j
    class DryRun implements SparkLauncher {
        private final List<SparkJobSpec> launched = new ArrayList<>();

        @Override
        public void launch(SparkJobSpec spec) {
            launched.add(spec);
            log.info("[outlet-batch][dry-run] 生成 Spark 作业(未提交):{} staging={} args={}",
                    spec.getJobName(), spec.getStagingPath(), spec.getArgs().size());
        }

        public List<SparkJobSpec> launched() {
            return launched;
        }
    }

    /** 本地进程提交(需真实 spark-submit 与集群) */
    @Slf4j
    class LocalProcess implements SparkLauncher {
        @Override
        public void launch(SparkJobSpec spec) {
            try {
                ProcessBuilder pb = new ProcessBuilder(spec.getArgs());
                pb.redirectErrorStream(true);
                Process p = pb.start();
                int code = p.waitFor();
                if (code != 0) {
                    throw new IllegalStateException("spark-submit 退出码 " + code + ":" + spec.getJobName());
                }
                log.info("[outlet-batch] spark-submit 完成:{} staging={}", spec.getJobName(), spec.getStagingPath());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("spark-submit 被中断", e);
            } catch (Exception e) {
                throw new IllegalStateException("spark-submit 启动失败:" + e.getMessage(), e);
            }
        }
    }
}

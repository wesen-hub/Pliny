import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TiDB 写入压测:复刻 dcp-synclink BatchWriteService/AtomicUpsertDao 的攒批写入形态
 *
 * 复刻点:
 *   1. 表结构 = ensureTable: _pk_id 自增主键 + 业务键唯一索引(uk_biz) + 字段列(VARCHAR) + writer + seq
 *   2. SQL = multiUpsertInternal: 多值 INSERT ... ON DUPLICATE KEY UPDATE
 *      字段级条件更新 col = IF(writer < VALUES(writer), VALUES(col), col)
 *   3. 攒批参数 = SynclinkComponents 默认: batch-size=500, write-threads 可调
 *   4. 分表 = 16 张物理表(模拟分表分散, 避免单表热点)
 *
 * 用法:
 *   java TiDbWriteBench <jdbcUrl> <threads> <batchSize> <totalRows> <conflictPct> <rowBytes>
 * 例:
 *   java TiDbWriteBench jdbc:mysql://127.0.0.1:14000/target_db?useSSL=false&allowPublicKeyRetrieval=true 4 500 2000000 0 1000
 */
public class TiDbWriteBench {

    static final int TABLES = 16;
    static final int FIELD_COLS = 10;   // 业务字段列数(1KB 行 ≈ 10 列 × 100B)

    public static void main(String[] args) throws Exception {
        String jdbcUrl = args[0];
        int threads = Integer.parseInt(args[1]);
        int batchSize = Integer.parseInt(args[2]);
        long totalRows = Long.parseLong(args[3]);
        int conflictPct = Integer.parseInt(args[4]);   // 0=纯新行(全量首写) 100=固定键池(增量更新)
        int rowBytes = Integer.parseInt(args[5]);       // 每行目标字节数(填充字段)

        // 建库建表
        try (Connection c = DriverManager.getConnection(jdbcUrl, "root", "root123");
             Statement st = c.createStatement()) {
            st.execute("CREATE DATABASE IF NOT EXISTS bench");
        }
        String benchUrl = jdbcUrl.contains("bench") ? jdbcUrl : jdbcUrl.replace("target_db", "bench");
        try (Connection c = DriverManager.getConnection(benchUrl, "root", "root123");
             Statement st = c.createStatement()) {
            for (int t = 0; t < TABLES; t++) {
                String ddl = "CREATE TABLE IF NOT EXISTS `t_order_" + t + "` ("
                        + "`_pk_id` BIGINT AUTO_INCREMENT PRIMARY KEY,"
                        + "`order_id` VARCHAR(255) NOT NULL DEFAULT '',"
                        + "`writer` VARCHAR(64) NOT NULL DEFAULT '',"
                        + "`f0` VARCHAR(255) NULL,`f1` VARCHAR(255) NULL,`f2` VARCHAR(255) NULL,"
                        + "`f3` VARCHAR(255) NULL,`f4` VARCHAR(255) NULL,`f5` VARCHAR(255) NULL,"
                        + "`f6` VARCHAR(255) NULL,`f7` VARCHAR(255) NULL,`f8` VARCHAR(255) NULL,"
                        + "`f9` VARCHAR(255) NULL,"
                        + "`seq` BIGINT NOT NULL DEFAULT 0,"
                        + "UNIQUE KEY uk_biz (`order_id`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bench'";
                st.execute(ddl);
            }
        }
        System.out.printf("[bench] 表就绪: %d 张 t_order_*, 线程=%d batch=%d 行数=%d 冲突=%d%% 行字节≈%d%n",
                TABLES, threads, batchSize, totalRows, conflictPct, rowBytes);

        long t0 = System.currentTimeMillis();
        CountDownLatch done = new CountDownLatch(threads);
        AtomicLong rows = new AtomicLong();
        AtomicLong batches = new AtomicLong();
        List<Worker> workers = new ArrayList<>();
        for (int w = 0; w < threads; w++) {
            Worker wk = new Worker(benchUrl, w, threads, batchSize, totalRows / threads,
                    conflictPct, rowBytes, rows, batches, done);
            workers.add(wk);
            wk.start();
        }
        // 每 5 秒打点
        long lastRows = 0, lastAt = t0;
        while (!done.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
            long now = System.currentTimeMillis();
            long r = rows.get();
            System.out.printf("  +%3ds 累计 %8d 行 | 瞬时 %8d 行/s | 批 %d%n",
                    (now - t0) / 1000, r, (r - lastRows) * 1000 / (now - lastAt), batches.get());
            lastRows = r;
            lastAt = now;
        }
        long dt = System.currentTimeMillis() - t0;
        long r = rows.get();
        System.out.printf("[RESULT] 总行数=%d 耗时=%.1fs 平均=%d 行/s(=%.1f MB/s @1KB) 批数=%d 平均批延迟=%.1fms%n",
                r, dt / 1000.0, r * 1000 / Math.max(1, dt), r * 1000.0 / Math.max(1, dt) / 1000.0,
                batches.get(), dt * threads / 1000.0 / Math.max(1, batches.get()));
        for (Worker w : workers) {
            if (w.ex != null) {
                w.ex.printStackTrace();
            }
        }
        System.exit(0);
    }

    static class Worker extends Thread {
        final String url;
        final int id, threads, batchSize;
        final long rowsToWrite;
        final int conflictPct, rowBytes;
        final AtomicLong rows, batches;
        final CountDownLatch done;
        volatile Exception ex;

        Worker(String url, int id, int threads, int batchSize, long rowsToWrite,
               int conflictPct, int rowBytes, AtomicLong rows, AtomicLong batches, CountDownLatch done) {
            super("bench-w" + id);
            this.url = url;
            this.id = id;
            this.threads = threads;
            this.batchSize = batchSize;
            this.rowsToWrite = rowsToWrite;
            this.conflictPct = conflictPct;
            this.rowBytes = rowBytes;
            this.rows = rows;
            this.batches = batches;
            this.done = done;
        }

        @Override
        public void run() {
            Random rnd = new Random(42L + id);
            char[] pad = new char[rowBytes / FIELD_COLS + 10];
            java.util.Arrays.fill(pad, 'x');
            String padStr = new String(pad);
            StringBuilder sql = new StringBuilder(64 * 1024);
            List<Object> params = new ArrayList<>(batchSize * 12);
            try (Connection c = DriverManager.getConnection(url, "root", "root123");
                 Statement st = c.createStatement()) {
                long written = 0;
                while (written < rowsToWrite) {
                    int n = (int) Math.min(batchSize, rowsToWrite - written);
                    // 键空间: 纯插入时各线程独立区间; 冲突时压到固定小键池
                    long keyBase = conflictPct > 0
                            ? rnd.nextInt(Math.max(1, (int) (rowsToWrite / 100 * (100 - conflictPct) / 100 + 1000)))
                            : (long) id * rowsToWrite + written;
                    int tableIdx = id % TABLES;
                    sql.setLength(0);
                    sql.append("INSERT INTO `t_order_").append(tableIdx).append("` ")
                            .append("(`order_id`,`writer`,`f0`,`f1`,`f2`,`f3`,`f4`,`f5`,`f6`,`f7`,`f8`,`f9`,`seq`) VALUES ");
                    params.clear();
                    for (int i = 0; i < n; i++) {
                        if (i > 0) sql.append(", ");
                        sql.append("(?,?,?,?,?,?,?,?,?,?,?,?,?)");
                        params.add("o" + (keyBase + i));
                        params.add("task1");
                        for (int f = 0; f < FIELD_COLS; f++) {
                            params.add(conflictPct > 0
                                    ? padStr.substring(0, Math.min(padStr.length(), rowBytes / FIELD_COLS))
                                    : padStr.substring(0, Math.min(padStr.length(), rowBytes / FIELD_COLS)));
                        }
                        params.add(System.nanoTime() / 1000);
                    }
                    sql.append(" ON DUPLICATE KEY UPDATE ");
                    for (int f = 0; f < FIELD_COLS; f++) {
                        if (f > 0) sql.append(", ");
                        sql.append("`f").append(f).append("` = IF(`writer` < VALUES(`writer`), VALUES(`f").append(f).append("`), `f").append(f).append("`)");
                    }
                    sql.append(", `writer` = GREATEST(`writer`, VALUES(`writer`))");
                    sql.append(", `seq` = GREATEST(`seq`, VALUES(`seq`))");
                    try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                        for (int i = 0; i < params.size(); i++) {
                            ps.setObject(i + 1, params.get(i));
                        }
                        ps.executeUpdate();
                    }
                    written += n;
                    rows.addAndGet(n);
                    batches.incrementAndGet();
                }
            } catch (Exception e) {
                this.ex = e;
            } finally {
                done.countDown();
            }
        }
    }
}

package com.dcp.module.outlet.sdk;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 位点表 JDBC 实现(表结构见 DDL 的 {@code consumer_offset},建在**消费方自己的库**)。
 *
 * <p>与业务写入同事务的做法:调用方把 {@link #save} 放在业务的 {@code @Transactional} 里,
 * 或使用 {@link OutletConsumer#commitWithBusinessWrite}。
 */
@Slf4j
@RequiredArgsConstructor
public class JdbcConsumerOffsetStore implements ConsumerOffsetStore {

    private final JdbcTemplate jdbc;

    @Override
    public long[] load(String consumerId, String streamId) {
        var rows = jdbc.queryForList(
                "SELECT epoch, position_tso FROM consumer_offset WHERE consumer_id = ? AND stream_id = ?",
                consumerId, streamId);
        if (rows.isEmpty()) {
            return null;
        }
        Object epoch = rows.get(0).get("epoch");
        Object tso = rows.get(0).get("position_tso");
        return new long[]{Long.parseLong(String.valueOf(epoch)), Long.parseLong(String.valueOf(tso))};
    }

    @Override
    public void save(String consumerId, String streamId, int epoch, long positionTso) {
        jdbc.update("INSERT INTO consumer_offset (consumer_id, stream_id, epoch, position_tso) VALUES (?,?,?,?) "
                        + "ON DUPLICATE KEY UPDATE epoch = VALUES(epoch), position_tso = VALUES(position_tso), "
                        + "updated_at = CURRENT_TIMESTAMP(3)",
                consumerId, streamId, epoch, positionTso);
    }
}

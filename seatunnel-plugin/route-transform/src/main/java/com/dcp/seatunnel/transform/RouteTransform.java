package com.dcp.seatunnel.transform;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * RouteTransform（设计文档 §4.3/§4.4）
 *
 * <p>职责单一: 只做「业务主键 → 桶号/物理表号」的路由计算, 追加三列:
 * {@code __bucket} / {@code __table_no} / {@code __target_table}。
 * 不掺入 seq 打标(入口侧做)与写入策略(sink 按配置做), 保持可独立测试。
 *
 * <p>位置: 放在 AviatorTransform 之后 —— 上游先把各源字段归一成统一业务主键
 * (如 systemB 的 orderNo → order_id), 这里只认归一后的字段。
 */
public class RouteTransform implements org.apache.seatunnel.api.transform.SeaTunnelMapTransform<SeaTunnelRow> {

    private final RouteTransformConfig config;

    public RouteTransform(RouteTransformConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "RouteTransform";
    }

    @Override
    public void open() {
        // 无资源需要初始化
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        Object[] fields = row.getFields();

        // 1. 取业务主键(空值按策略: ERROR 抛异常 / FALLBACK 落兜底键)
        Object rawKey = config.getBusinessKeyIndex() < fields.length
                ? fields[config.getBusinessKeyIndex()] : null;
        String key = RouteUtil.resolveKey(rawKey, config.getNullKeyMode(), config.getFallbackValue());

        // 2. 路由计算: 桶号 + 物理表号(同一 key 永远同一桶同一表)
        RouteUtil.Route route = RouteUtil.route(key, config.getBucketCount(), config.getTableCount());

        // 3. 拼物理表名: t_order_03(表号补零, 宽度自动按表数)
        String targetTable = config.getTablePrefix() + "_"
                + String.format("%0" + config.getTableNameWidth() + "d", route.tableNo());

        // 4. 输出 = 原字段 + 路由三列
        SeaTunnelRow out = new SeaTunnelRow(fields.length + 3);
        System.arraycopy(fields, 0, out.getFields(), 0, fields.length);
        out.setField(fields.length, route.bucket());
        out.setField(fields.length + 1, route.tableNo());
        out.setField(fields.length + 2, targetTable);
        return out;
    }

    @Override
    public void close() {
        // 无资源需要释放
    }
}

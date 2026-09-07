package com.dcp.seatunnel.transform;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * AviatorTransform（设计文档 §6.2, SeaTunnel 2.3.12 SPI）
 * 配置:
 *   mappings: [{sourceField, targetField, transformType(direct|dictionary|expression), transformRule}]
 *   dict_data: {字典名: {源值: 目标值}}   (可选)
 *   fail_on_error: false
 * 特性: 编译一次执行 N 次; 按 sortNo 顺序执行, 表达式可引用源字段与前置输出; 安全沙箱(禁反射)
 */
public class AviatorTransform implements org.apache.seatunnel.api.transform.SeaTunnelMapTransform<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private final AviatorTransformConfig config;
    /** 构造时初始化（不依赖引擎回调 open(), 提交阶段即为空, 序列化后 worker 端重建） */
    private transient java.util.Map<String, com.googlecode.aviator.Expression> compiled = new java.util.LinkedHashMap<>();
    private transient com.googlecode.aviator.AviatorEvaluatorInstance evaluator;

    public AviatorTransform(AviatorTransformConfig config) {
        this.config = config;
        init();
    }

    private void init() {
        if (evaluator == null) {
            this.evaluator = ExpressionUtil.newSafeEvaluator(config.getDictData());
        }
        if (compiled == null) {
            compiled = new java.util.LinkedHashMap<>();
        }
        compiled.clear();
        for (AviatorTransformConfig.Mapping m : config.getMappings()) {
            if ("expression".equals(m.getTransformType())
                    && m.getTransformRule() != null && !m.getTransformRule().isBlank()) {
                compiled.put(m.getTargetField(), evaluator.compile(m.getTransformRule()));
            }
        }
    }

    @Override
    public String getPluginName() {
        return "AviatorTransform";
    }

    @Override
    public void open() {
        init(); // 幂等: 引擎若回调 open() 则重建（worker 端反序列化后首次 map 前）
    }

    @Override
    public java.util.List<org.apache.seatunnel.api.table.catalog.CatalogTable> getProducedCatalogTables() {
        return java.util.List.of(config.buildOutputCatalogTable());
    }

    @Override
    public org.apache.seatunnel.api.table.catalog.CatalogTable getProducedCatalogTable() {
        return config.buildOutputCatalogTable();
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        Object[] fields = row.getFields();
        String[] inNames = config.getInputNames();
        java.util.Map<String, Object> env = new java.util.LinkedHashMap<>();
        for (int i = 0; i < fields.length && i < inNames.length; i++) {
            // 原始值入 env: direct/dictionary 原样透传（normalize 会破坏 LocalDateTime/Timestamp 类型, 导致 sink 转型失败）
            env.put(inNames[i], fields[i]);
        }
        java.util.Map<String, Object> outputs = new java.util.LinkedHashMap<>();
        for (AviatorTransformConfig.Mapping m : config.getMappings()) {
            Object value;
            try {
                value = switch (m.getTransformType()) {
                    case "dictionary" -> {
                        Object raw = env.get(m.getSourceField());
                        java.util.Map<String, String> dict = config.getDictData().get(m.getTransformRule());
                        if (dict != null && raw != null) {
                            String mapped = dict.get(String.valueOf(raw));
                            yield mapped != null ? mapped : raw;
                        }
                        yield raw;
                    }
                    case "expression" -> {
                        com.googlecode.aviator.Expression expr = compiled.get(m.getTargetField());
                        if (expr == null) {
                            Object raw = env.get(m.getSourceField());
                            yield raw == null ? null : String.valueOf(raw);
                        }
                        // Aviator 求值用 normalize 副本（Date/数值友好）; 不污染 env 原始值
                        java.util.Map<String, Object> aEnv = ExpressionUtil.normalizeEnv(env);
                        Object v = expr.execute(aEnv);
                        if (v instanceof com.googlecode.aviator.runtime.type.AviatorObject ao) {
                            v = ao.getValue(aEnv);
                        }
                        // 输出 schema 为 STRING（表达式列固定字符串化, 由目标库隐式转换）
                        v = ExpressionUtil.normalize(v);
                        yield v == null ? null : String.valueOf(v);
                    }
                    default -> env.get(m.getSourceField());
                };
            } catch (Exception e) {
                if (config.isFailOnError()) {
                    throw new RuntimeException("AviatorTransform 字段[" + m.getTargetField() + "] 转换失败: " + e.getMessage(), e);
                }
                value = null; // fail_on_error=false 行级容错
            }
            outputs.put(m.getTargetField(), value);
            env.put(m.getTargetField(), value); // 后者可引用前者输出
        }
        SeaTunnelRow out = new SeaTunnelRow(config.getOutputNames().length);
        for (int i = 0; i < config.getOutputNames().length; i++) {
            out.setField(i, outputs.get(config.getOutputNames()[i]));
        }
        return out;
    }

    @Override
    public void close() {
        // nothing to release
    }
}

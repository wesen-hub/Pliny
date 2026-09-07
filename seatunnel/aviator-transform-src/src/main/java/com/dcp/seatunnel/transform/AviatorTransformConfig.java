package com.dcp.seatunnel.transform;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** AviatorTransform 配置: 解析 mappings/dict_data, 构建输出 CatalogTable */
public class AviatorTransformConfig implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    public static class Mapping implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final String sourceField;
        private final String targetField;
        private final String transformType; // direct|dictionary|expression
        private final String transformRule;
        private final int sortNo;

        public Mapping(String sourceField, String targetField, String transformType,
                       String transformRule, int sortNo) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.transformType = transformType == null ? "direct" : transformType;
            this.transformRule = transformRule;
            this.sortNo = sortNo;
        }

        public String getSourceField() { return sourceField; }
        public String getTargetField() { return targetField; }
        public String getTransformType() { return transformType; }
        public String getTransformRule() { return transformRule; }
        public int getSortNo() { return sortNo; }
    }

    private final List<Mapping> mappings;
    private final Map<String, Map<String, String>> dictData;
    private final boolean failOnError;
    private final String[] inputNames;
    private final String[] outputNames;
    private final CatalogTable outputTable;

    private static final ObjectMapper OM = new ObjectMapper();

    @SuppressWarnings("unchecked")
    public AviatorTransformConfig(List<CatalogTable> inputTables,
                                  org.apache.seatunnel.api.configuration.ReadonlyConfig options) {
        CatalogTable input = inputTables.get(0);
        SeaTunnelRowType inType = input.getSeaTunnelRowType();
        this.inputNames = inType.getFieldNames();
        this.dictData = new LinkedHashMap<>();

        // 解析 mappings（JSON 字符串: {"mappings":[...]}, 嵌套结构会破坏 Zeta DAG 序列化）
        List<Mapping> parsed = new ArrayList<>();
        Object rawRules = options.get(AviatorTransformFactory.MAPPING_RULES);
        if (rawRules instanceof String s && !s.isBlank()) {
            try {
                JsonNode root = OM.readTree(s);
                JsonNode arr = root.path("mappings");
                if (arr.isArray()) {
                    for (JsonNode m : arr) {
                        parsed.add(new Mapping(
                                text(m, "sourceField"),
                                text(m, "targetField"),
                                text(m, "transformType"),
                                m.hasNonNull("transformRule") ? m.get("transformRule").asText() : null,
                                m.path("sortNo").asInt(0)));
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("mapping_rules JSON 解析失败: " + e.getMessage(), e);
            }
        }
        parsed.sort(java.util.Comparator.comparingInt(Mapping::getSortNo));
        this.mappings = parsed;

        // 解析 dict_data（JSON 字符串: {"data":{...}}, 可选）
        Object rawDict = options.get(AviatorTransformFactory.DICT_DATA);
        if (rawDict instanceof String s && !s.isBlank()) {
            try {
                JsonNode data = OM.readTree(s).path("data");
                if (data.isObject()) {
                    Iterator<Map.Entry<String, JsonNode>> it = data.fields();
                    while (it.hasNext()) {
                        Map.Entry<String, JsonNode> e = it.next();
                        Map<String, String> m = new LinkedHashMap<>();
                        Iterator<Map.Entry<String, JsonNode>> kv = e.getValue().fields();
                        while (kv.hasNext()) {
                            Map.Entry<String, JsonNode> en = kv.next();
                            m.put(en.getKey(), en.getValue().asText());
                        }
                        dictData.put(e.getKey(), m);
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("dict_data JSON 解析失败: " + e.getMessage(), e);
            }
        }
        this.failOnError = options.get(AviatorTransformFactory.FAIL_ON_ERROR);

        // 输出 schema: 目标字段
        this.outputNames = parsed.stream().map(Mapping::getTargetField).toArray(String[]::new);
        List<org.apache.seatunnel.api.table.catalog.Column> cols = new ArrayList<>();
        SeaTunnelDataType<?>[] inTypes = inType.getFieldTypes();
        for (Mapping m : parsed) {
            SeaTunnelDataType<?> dt = BasicType.STRING_TYPE;
            int idx = inType.indexOf(m.getSourceField());
            if ("expression".equals(m.getTransformType())) {
                dt = BasicType.STRING_TYPE;
            } else if (idx >= 0 && idx < inTypes.length) {
                dt = inTypes[idx];
            }
            cols.add(PhysicalColumn.of(m.getTargetField(), dt, (Long) null, true, null, null));
        }
        TableSchema schema = TableSchema.builder().columns(cols).build();
        this.outputTable = CatalogTable.of(input.getTableId(), schema, input.getOptions(),
                input.getPartitionKeys(), input.getComment());
    }

    public List<Mapping> getMappings() { return mappings; }
    public Map<String, Map<String, String>> getDictData() { return dictData; }
    public boolean isFailOnError() { return failOnError; }
    public String[] getInputNames() { return inputNames; }
    public String[] getOutputNames() { return outputNames; }
    public CatalogTable buildOutputCatalogTable() { return outputTable; }

    private static String text(JsonNode n, String key) {
        return n.hasNonNull(key) ? n.get(key).asText() : null;
    }
}

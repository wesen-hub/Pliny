package com.dcp.seatunnel.transform;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** AviatorTransform 配置: 解析 mappings/dict_data, 构建输出 CatalogTable */
public class AviatorTransformConfig {

    public static class Mapping {
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

    @SuppressWarnings("unchecked")
    public AviatorTransformConfig(List<CatalogTable> inputTables,
                                  org.apache.seatunnel.api.configuration.ReadonlyConfig options) {
        CatalogTable input = inputTables.get(0);
        SeaTunnelRowType inType = input.getSeaTunnelRowType();
        this.inputNames = inType.getFieldNames();
        this.dictData = new LinkedHashMap<>();

        // 解析 mappings
        List<Mapping> parsed = new ArrayList<>();
        Object rawMappings = options.get(AviatorTransformFactory.MAPPINGS);
        if (rawMappings instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof Map<?, ?> m) {
                    int sort = m.get("sortNo") instanceof Number n ? n.intValue() : 0;
                    parsed.add(new Mapping(
                            str(m.get("sourceField")),
                            str(m.get("targetField")),
                            str(m.get("transformType")),
                            str(m.get("transformRule")),
                            sort));
                }
            }
        }
        parsed.sort(java.util.Comparator.comparingInt(Mapping::getSortNo));
        this.mappings = parsed;

        // 解析 dict_data
        Object rawDict = options.get(AviatorTransformFactory.DICT_DATA);
        if (rawDict instanceof Map<?, ?> dictMap) {
            for (Map.Entry<?, ?> e : dictMap.entrySet()) {
                Map<String, String> m = new LinkedHashMap<>();
                if (e.getValue() instanceof Map<?, ?> inner) {
                    inner.forEach((k, v) -> m.put(String.valueOf(k), String.valueOf(v)));
                }
                dictData.put(String.valueOf(e.getKey()), m);
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

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }
}

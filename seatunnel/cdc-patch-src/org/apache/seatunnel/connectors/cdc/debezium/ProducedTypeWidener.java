package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * producedType 加宽补丁辅助类(PatchCdcBasePosition 注入到 IncrementalSource.getProducedCatalogTables)。
 *
 * <p>背景: 位点透传补丁给 deserialize 行尾追加 3 个 metadata 列(__src_pos 位点字符串 /
 * __snapshot 快照标志 / __src_before before 行 JSON), 行宽 = 物理列 + 3; 而官方 producedType
 * ({@code CatalogTable.getSeaTunnelRowType()} = {@code TableSchema.toPhysicalRowDataType()})
 * 只含物理列 —— 引擎 SeaTunnelSourceCollector 按 producedType 逐字段计算行字节数,
 * 行宽超过 rowType 宽度即 {@code ArrayIndexOutOfBoundsException}。
 *
 * <p>本类把 producedType 的每个 CatalogTable 同步加宽 3 个物理列(列名与补丁注入的
 * metadata 列对应), 使行宽与 rowType 宽度一致。下游 CdcSeqTransform 按
 * {@code values[names.length] / values[names.length + 1] / values[names.length + 2]}
 * 读取位点/快照标志/before 行, 不受列名影响(metadata 列不进业务 schema)。
 */
public final class ProducedTypeWidener implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SRC_POS_COLUMN = "__src_pos";
    public static final String SNAPSHOT_COLUMN = "__snapshot";
    public static final String BEFORE_COLUMN = "__src_before";

    private ProducedTypeWidener() {
    }

    /** 每个 CatalogTable 的物理列末尾追加 __src_pos(STRING) + __snapshot(BOOLEAN) + __src_before(STRING)
     *  (缺陷 6d: 三列 nullable=true — metadata 列部分路径为 null(如 mongo emit 位点可空),
     *  NOT NULL 标记与行数据不符, 空值强校验的引擎可能误报) */
    public static List<CatalogTable> widen(List<CatalogTable> tables) {
        if (tables == null || tables.isEmpty()) {
            return tables;
        }
        List<CatalogTable> out = new ArrayList<>(tables.size());
        for (CatalogTable t : tables) {
            out.add(widenTable(t));
        }
        return out;
    }

    private static CatalogTable widenTable(CatalogTable t) {
        TableSchema schema = t == null ? null : t.getTableSchema();
        if (schema == null) {
            return t;
        }
        List<Column> cols = new ArrayList<>(schema.getColumns());
        cols.add(PhysicalColumn.of(SRC_POS_COLUMN, BasicType.STRING_TYPE, (Long) null, true, null, ""));
        cols.add(PhysicalColumn.of(SNAPSHOT_COLUMN, BasicType.BOOLEAN_TYPE, (Long) null, true, null, ""));
        cols.add(PhysicalColumn.of(BEFORE_COLUMN, BasicType.STRING_TYPE, (Long) null, true, null, ""));
        TableSchema wide = TableSchema.builder()
                .columns(cols)
                .primaryKey(schema.getPrimaryKey())
                .constraintKey(schema.getConstraintKeys())
                .build();
        return CatalogTable.of(t.getTableId(), wide, t.getOptions(), t.getPartitionKeys(), t.getComment());
    }
}

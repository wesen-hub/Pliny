package com.dcp.engine;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MongoDB 字段类型推断：采样集合首条文档，BSON 类型 -> SeaTunnel schema 类型。
 * 缺失字段/嵌套对象/未知类型一律回退 string（由目标库隐式转换）。
 * 设计文档 §5.1（Mongo 源 schema 必填，官方 connector 不自动推断）。
 */
public final class MongoSchemaUtil {

    private MongoSchemaUtil() {
    }

    /** 推断字段类型（field -> seatunnel 类型字符串），失败时全部回退 string */
    public static Map<String, String> inferFields(String uri, String database, String collection,
                                                  List<String> fieldNames) {
        Map<String, String> result = new LinkedHashMap<>();
        if (fieldNames == null) {
            return result;
        }
        for (String f : fieldNames) {
            result.put(f, "string");
        }
        try (MongoClient client = MongoClients.create(uri)) {
            MongoCollection<Document> coll = client.getDatabase(database).getCollection(collection);
            Document sample = coll.find().first();
            if (sample == null) {
                return result;
            }
            for (String f : fieldNames) {
                Object v = resolve(sample, f);
                if (v != null) {
                    result.put(f, seatunnelType(v));
                }
            }
        } catch (Exception ignored) {
            // 采样失败按 string 兜底, 写入由目标库隐式转换
        }
        return result;
    }

    /** 支持点号路径（嵌套字段 a.b.c） */
    private static Object resolve(Document doc, String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        if (!path.contains(".")) {
            return doc.get(path);
        }
        Object cur = doc;
        for (String part : path.split("\\.")) {
            if (!(cur instanceof Document d)) {
                return null;
            }
            cur = d.get(part);
        }
        return cur;
    }

    private static String seatunnelType(Object v) {
        if (v instanceof org.bson.types.ObjectId) return "string";
        if (v instanceof String) return "string";
        if (v instanceof Boolean) return "boolean";
        if (v instanceof Integer) return "int";
        if (v instanceof Long) return "bigint";
        if (v instanceof Double || v instanceof Float) return "double";
        if (v instanceof java.math.BigDecimal || v instanceof org.bson.types.Decimal128) return "decimal(34, 18)";
        if (v instanceof java.util.Date) return "timestamp";
        return "string";
    }
}

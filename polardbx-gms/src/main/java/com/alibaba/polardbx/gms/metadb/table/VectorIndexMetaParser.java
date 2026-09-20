package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Conversion and validation for vector index metadata.
 */
public final class VectorIndexMetaParser {

    public static final String VECTOR_INDEX_TYPE = "VECTOR";

    private static final String M = "M";
    private static final String DISTANCE = "DISTANCE";
    private static final String EF_CONSTRUCTION = "EF_CONSTRUCTION";
    private static final String DIM = "DIM";

    private VectorIndexMetaParser() {
    }

    public static IndexesInfoSchemaRecord toIndexesInfoSchemaRecord(VectorIndexesInfoSchemaRecord vectorRecord) {
        requireText(vectorRecord.tableSchema, "TABLE_SCHEMA");
        requireText(vectorRecord.tableName, "TABLE_NAME");
        requireText(vectorRecord.indexName, "INDEX_NAME");
        requireText(vectorRecord.columnName, "COLUMN_NAME");

        VectorIndexMeta vectorIndexMeta = fromInfoSchemaRecord(vectorRecord);

        IndexesInfoSchemaRecord record = new IndexesInfoSchemaRecord();
        record.tableSchema = vectorRecord.tableSchema;
        record.tableName = vectorRecord.tableName;
        record.nonUnique = 1;
        record.indexSchema = vectorRecord.tableSchema;
        record.indexName = vectorRecord.indexName;
        record.seqInIndex = 1;
        record.columnName = vectorRecord.columnName;
        record.collation = null;
        record.cardinality = 0;
        record.subPart = 0;
        record.packed = null;
        record.nullable = "";
        record.indexType = VECTOR_INDEX_TYPE;
        record.comment = "";
        record.indexComment = toCanonicalIndexComment(vectorIndexMeta);
        return record;
    }

    public static VectorIndexMeta fromInfoSchemaRecord(VectorIndexesInfoSchemaRecord record) {
        requireText(record.algorithm, "ALGORITHM");
        requireText(record.metricType, "METRIC_TYPE");
        requireText(record.m, M);
        requireText(record.efConstruction, EF_CONSTRUCTION);
        if (record.dimension == null) {
            throw corrupt("missing vector index field DIMENSION");
        }

        VectorIndexMeta.Algorithm algorithm = parseVectorAlgorithm(record.algorithm);
        return createVectorIndexMeta(algorithm, record.metricType, record.dimension.toString(), record.m,
            record.efConstruction);
    }

    private static VectorIndexMeta.Algorithm parseVectorAlgorithm(String algorithm) {
        try {
            return VectorIndexMeta.Algorithm.valueOf(StringUtils.trimToEmpty(algorithm).toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw corrupt("unsupported vector index algorithm " + algorithm);
        }
    }

    public static VectorIndexMeta mergeVectorIndexMeta(VectorIndexMeta existing, String indexComment,
                                                       String indexName) {
        if (StringUtils.isBlank(indexComment) && existing != null) {
            return existing;
        }
        VectorIndexMeta resolved = parseVectorIndexMeta(indexComment);
        if (existing != null && !existing.equals(resolved)) {
            throw corrupt("conflicting Index_comment values for vector index " + indexName);
        }
        return resolved;
    }

    public static VectorIndexMeta parseVectorIndexMeta(String indexComment) {
        Map<String, String> options = parseOptions(indexComment);
        return createVectorIndexMeta(VectorIndexMeta.Algorithm.HNSW, options.get(DISTANCE), options.get(DIM),
            options.get(M), options.get(EF_CONSTRUCTION));
    }

    public static String toCanonicalIndexComment(VectorIndexMeta meta) {
        if (meta == null) {
            throw corrupt("missing vector index metadata");
        }
        return M + '=' + meta.getM()
            + ", " + DISTANCE + '=' + meta.getMetric().name()
            + ", " + EF_CONSTRUCTION + '=' + meta.getEfConstruction()
            + ", " + DIM + '=' + meta.getDimension();
    }

    private static Map<String, String> parseOptions(String comment) {
        if (StringUtils.isBlank(comment)) {
            throw corrupt("missing vector index metadata");
        }
        Map<String, String> options = new LinkedHashMap<>();
        for (String part : comment.split(",", -1)) {
            int separator = part.indexOf('=');
            if (separator <= 0 || separator == part.length() - 1 || part.indexOf('=', separator + 1) >= 0) {
                throw corrupt("malformed vector index metadata: " + comment);
            }
            String key = part.substring(0, separator).trim().toUpperCase(Locale.ROOT);
            String value = part.substring(separator + 1).trim();
            if (key.isEmpty() || value.isEmpty() || options.putIfAbsent(key, value) != null) {
                throw corrupt("duplicate or empty vector index metadata key: " + key);
            }
            if (!M.equals(key) && !DISTANCE.equals(key) && !EF_CONSTRUCTION.equals(key) && !DIM.equals(key)) {
                throw corrupt("unknown vector index metadata key " + key);
            }
        }

        requireOption(options, M);
        requireOption(options, DISTANCE);
        requireOption(options, EF_CONSTRUCTION);
        requireOption(options, DIM);
        return Collections.unmodifiableMap(options);
    }

    private static void requireOption(Map<String, String> options, String key) {
        if (!options.containsKey(key)) {
            throw corrupt("missing vector index metadata key " + key);
        }
    }

    private static VectorIndexMeta createVectorIndexMeta(VectorIndexMeta.Algorithm algorithm, String metric,
                                                         String dimension,
                                                         String m, String efConstruction) {
        final VectorIndexMeta.Metric parsedMetric;
        try {
            parsedMetric = VectorIndexMeta.Metric.valueOf(metric.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw corrupt("invalid vector index distance " + metric);
        }
        int parsedDimension = parseInteger(DIM, dimension, 1, Integer.MAX_VALUE);
        int parsedM = parseInteger(M, m, 3, 200);
        int parsedEfConstruction = parseInteger(EF_CONSTRUCTION, efConstruction, 5, 1000);
        return new VectorIndexMeta(algorithm, parsedMetric, parsedDimension, parsedM, parsedEfConstruction);
    }

    private static int parseInteger(String key, String value, int minimum, int maximum) {
        final int number;
        try {
            number = Integer.parseInt(value);
        } catch (NumberFormatException | NullPointerException e) {
            throw corrupt("invalid vector index metadata value " + key + '=' + value);
        }
        if (number < minimum || number > maximum) {
            throw corrupt("vector index metadata value out of range " + key + '=' + value);
        }
        return number;
    }

    private static void requireText(String value, String field) {
        if (StringUtils.isBlank(value)) {
            throw corrupt("missing vector index field " + field);
        }
    }

    private static TddlRuntimeException corrupt(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch", detail);
    }
}

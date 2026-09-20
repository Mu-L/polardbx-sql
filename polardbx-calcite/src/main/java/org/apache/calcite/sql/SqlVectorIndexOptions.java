package org.apache.calcite.sql;

import java.util.Locale;
import java.util.Map;

/**
 * Immutable, validated options for a DN HNSW vector index.
 */
public final class SqlVectorIndexOptions {

    public enum DistanceMetric {
        EUCLIDEAN,
        COSINE,
        INNER_PRODUCT;

        static DistanceMetric parse(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported vector distance metric: " + value, e);
            }
        }
    }

    private final DistanceMetric distance;
    private final Integer m;
    private final Integer efConstruction;

    private SqlVectorIndexOptions(DistanceMetric distance, Integer m, Integer efConstruction) {
        this.distance = distance;
        this.m = validateRange("M", m, 3, 200);
        this.efConstruction = validateRange("EF_CONSTRUCTION", efConstruction, 5, 1000);
    }

    public static SqlVectorIndexOptions from(Map<String, String> options) {
        if (options == null || options.isEmpty()) {
            return null;
        }
        for (String key : options.keySet()) {
            if (!"distance".equalsIgnoreCase(key)
                && !"m".equalsIgnoreCase(key)
                && !"ef_construction".equalsIgnoreCase(key)) {
                throw new IllegalArgumentException("Unsupported vector index option: " + key);
            }
        }
        DistanceMetric distance = DistanceMetric.parse(getIgnoreCase(options, "distance"));
        Integer m = parseInteger("M", getIgnoreCase(options, "m"));
        Integer efConstruction = parseInteger(
            "EF_CONSTRUCTION", getIgnoreCase(options, "ef_construction"));
        return new SqlVectorIndexOptions(distance, m, efConstruction);
    }

    private static String getIgnoreCase(Map<String, String> options, String target) {
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (target.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static Integer parseInteger(String name, String value) {
        if (value == null) {
            return null;
        }
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid vector index " + name + ": " + value, e);
        }
    }

    private static Integer validateRange(String name, Integer value, int min, int max) {
        if (value != null && (value < min || value > max)) {
            throw new IllegalArgumentException(
                "Vector index " + name + " must be between " + min + " and " + max + ": " + value);
        }
        return value;
    }

    public DistanceMetric getDistance() {
        return distance;
    }

    public Integer getM() {
        return m;
    }

    public Integer getEfConstruction() {
        return efConstruction;
    }
}

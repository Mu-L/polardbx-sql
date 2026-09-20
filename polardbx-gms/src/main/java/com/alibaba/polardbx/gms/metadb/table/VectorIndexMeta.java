package com.alibaba.polardbx.gms.metadb.table;

import java.io.Serializable;
import java.util.Objects;

/**
 * Resolved physical metadata for a vector index.
 */
public final class VectorIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Algorithm {
        HNSW
    }

    public enum Metric {
        EUCLIDEAN,
        COSINE,
        INNER_PRODUCT
    }

    private final Algorithm algorithm;
    private final Metric metric;
    private final int dimension;
    private final int m;
    private final int efConstruction;

    public VectorIndexMeta(Algorithm algorithm, Metric metric, int dimension, int m, int efConstruction) {
        this.algorithm = Objects.requireNonNull(algorithm, "algorithm is null");
        this.metric = Objects.requireNonNull(metric, "metric is null");
        if (dimension < 1) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        if (m < 3 || m > 200) {
            throw new IllegalArgumentException("m must be between 3 and 200");
        }
        if (efConstruction < 5 || efConstruction > 1000) {
            throw new IllegalArgumentException("efConstruction must be between 5 and 1000");
        }
        this.dimension = dimension;
        this.m = m;
        this.efConstruction = efConstruction;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public Metric getMetric() {
        return metric;
    }

    public int getDimension() {
        return dimension;
    }

    public int getM() {
        return m;
    }

    public int getEfConstruction() {
        return efConstruction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VectorIndexMeta)) {
            return false;
        }
        VectorIndexMeta that = (VectorIndexMeta) o;
        return dimension == that.dimension
            && m == that.m
            && efConstruction == that.efConstruction
            && algorithm == that.algorithm
            && metric == that.metric;
    }

    @Override
    public int hashCode() {
        return Objects.hash(algorithm, metric, dimension, m, efConstruction);
    }

    @Override
    public String toString() {
        return "VectorIndexMeta{" +
            "algorithm=" + algorithm +
            ", metric=" + metric +
            ", dimension=" + dimension +
            ", m=" + m +
            ", efConstruction=" + efConstruction +
            '}';
    }
}

package org.apache.calcite;

import org.apache.calcite.rel.logical.LogicalCTEProducer;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CTEConverterContext {
    private final Map<SqlValidatorNamespace, LogicalCTEProducer> cteNameToProducer;

    private final AtomicInteger cteIdGenerator;

    private final Map<Integer, Integer> cteConsumerCount;

    private int nestingDepth;

    public CTEConverterContext() {
        cteNameToProducer = new HashMap<>();
        cteIdGenerator = new AtomicInteger(-1);
        cteConsumerCount = new HashMap<>();
        nestingDepth = 0;
    }

    private CTEConverterContext(Map<SqlValidatorNamespace, LogicalCTEProducer> cteNameToProducer,
                                AtomicInteger cteIdGenerator, Map<Integer, Integer> cteConsumerCount, int nestingDepth) {
        this.cteNameToProducer = new HashMap<>(cteNameToProducer);
        this.cteIdGenerator = cteIdGenerator;
        this.cteConsumerCount = cteConsumerCount;
        this.nestingDepth = nestingDepth;
    }

    public void registerCteProducer(SqlValidatorNamespace namespace, LogicalCTEProducer producer) {
        cteNameToProducer.put(namespace, producer);
    }

    public LogicalCTEProducer getCte(SqlValidatorNamespace namespace) {
        return cteNameToProducer.get(namespace);
    }

    public int nextCteId() {
        return cteIdGenerator.incrementAndGet();
    }

    public Integer registerCteConsumer(int cteId) {
        Integer current = cteConsumerCount.getOrDefault(cteId, -1) + 1;
        cteConsumerCount.put(cteId, current);
        return current;
    }

    public int getNestingDepth() {
        return nestingDepth;
    }

    public void incrementNestingDepth() {
        nestingDepth++;
    }

    public void decrementNestingDepth() {
        nestingDepth--;
    }

    public CTEConverterContext copy() {
        return new CTEConverterContext(cteNameToProducer, cteIdGenerator, cteConsumerCount, nestingDepth);
    }
}

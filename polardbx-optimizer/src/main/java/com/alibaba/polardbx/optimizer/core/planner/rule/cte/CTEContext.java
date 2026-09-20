package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CTEConsumer;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalCTEProducer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class CTEContext {
    private boolean inited = false;

    private final Map<Integer, CTEProducer> cteProducerMap = Maps.newHashMap();

    private final Map<Integer, Integer> cteConsumerCount = Maps.newHashMap();

    private final Map<Integer, Integer> cteConsumerMaxSn = Maps.newHashMap();

    private final Map<Integer, Set<LogicalCTEConsumer>> logicalCteConsumersMap = Maps.newHashMap();

    private int maxCteId = -1;

    private boolean enableCteReuse = false;

    private CTEMode cteMode = CTEMode.INLINE;

    private int reuseThreshold = 1;

    public void forceInit(RelNode root, PlannerContext pc) {
        enableCteReuse = DynamicConfig.getInstance().isEnableCTEReuse();
        cteMode = CTEMode.parse(pc.getParamManager().getString(ConnectionParams.CTE_MODE));
        reuseThreshold = pc.getParamManager().getInt(ConnectionParams.CTE_REUSE_THRESHOLD);
        clear();
        CTEUtil.collectCte(root, this);
    }

    public void init() {
        if (inited) {
            return;
        }
        inited = true;
        clear();
    }

    public Integer nextCteId() {
        if (!enableCteReuse) {
            return null;
        }
        return ++maxCteId;
    }

    public void registerCteProducer(CTEProducer cteProducer) {
        cteProducerMap.put(cteProducer.getCteId(), cteProducer);
        maxCteId = Math.max(maxCteId, cteProducer.getCteId());
    }

    public void registerCteConsumer(CTEConsumer cteConsumer) {
        Integer cteId = cteConsumer.getCteId();
        Integer current = cteConsumerMaxSn.getOrDefault(cteId, 0);
        cteConsumerMaxSn.put(cteId, Math.max(current, cteConsumer.getSn()));
        cteConsumerCount.put(cteId, cteConsumerCount.getOrDefault(cteId, 0) + 1);
        maxCteId = Math.max(maxCteId, cteId);
        if (cteConsumer instanceof LogicalCTEConsumer) {
            logicalCteConsumersMap.computeIfAbsent(cteId, k -> Sets.newHashSet()).add(
                (LogicalCTEConsumer) cteConsumer);
        }
    }

    public Integer getNextCteConsumerSn(int cteId) {
        Integer current = cteConsumerMaxSn.getOrDefault(cteId, -1) + 1;
        cteConsumerMaxSn.put(cteId, current);
        return current;
    }

    public CTEProducer getCteProducer(Integer cteId) {
        return cteProducerMap.get(cteId);
    }

    public Collection<LogicalCTEConsumer> getCteConsumers(Integer cteId) {
        return logicalCteConsumersMap.getOrDefault(cteId, Collections.emptySet());
    }

    public void replaceCteConsumer(LogicalCTEConsumer oldCTEConsumer, LogicalCTEConsumer newCteConsumer) {
        Set<LogicalCTEConsumer> consumers =
            logicalCteConsumersMap.computeIfAbsent(oldCTEConsumer.getCteId(), k -> Sets.newHashSet());
        consumers.remove(oldCTEConsumer);
        consumers.add(newCteConsumer);
    }

    public void clear() {
        cteProducerMap.clear();
        cteConsumerCount.clear();
        logicalCteConsumersMap.clear();
        cteConsumerMaxSn.clear();
        maxCteId = -1;
    }

    /**
     * Re-collect all CTE info from the tree.
     * Used after pushFilterProject to get updated consumers with their filter/project info.
     */
    public void reCollect(RelNode root) {
        clear();
        CTEUtil.collectCte(root, this);
    }

    /**
     * Update cached producer and consumers for a given cteId after reconstruction.
     */
    public void updateAfterReconstruct(int cteId, LogicalCTEProducer newProducer,
                                       Set<LogicalCTEConsumer> newConsumers) {
        cteProducerMap.put(cteId, newProducer);
        logicalCteConsumersMap.put(cteId, newConsumers);
        cteConsumerCount.put(cteId, newConsumers.size());
    }

    public boolean hasCTE() {
        return !cteProducerMap.isEmpty();
    }

    public int inlineCount() {
        int cnt = 0;
        for (Integer cteId : cteProducerMap.keySet()) {
            if (shouldInline(cteId)) {
                cnt++;
            }
        }
        return cnt;
    }

    public boolean shouldInline(Integer cteId) {
        if (!enableCteReuse) {
            return false;
        }
        if (cteMode == CTEMode.INLINE) {
            return true;
        }
        if (cteId == null) {
            return true;
        }
        Integer consumerCount = cteConsumerCount.get(cteId);
        if (consumerCount == null) {
            return true;
        }
        if (consumerCount == 0) {
            return true;
        }
        if (cteMode == CTEMode.REUSE) {
            return false;
        }
        if (consumerCount == 1) {
            return true;
        }
        if (consumerCount <= reuseThreshold) {
            return true;
        }
        return false;
    }

    public boolean isEnableCteReuse() {
        return enableCteReuse;
    }
}

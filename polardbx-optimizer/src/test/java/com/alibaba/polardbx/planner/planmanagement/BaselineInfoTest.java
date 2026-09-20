package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectShardingKeyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_RECENTLY_EXECUTED_PERIOD;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class BaselineInfoTest {
    static AtomicInteger planId = new AtomicInteger();

    @Before
    public void prepare() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    public void testEmptyBaselineClear() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());

        // test clear unfixed plan to empty
        b1.addAcceptedPlan(buildPlan());
        Assert.assertTrue(b1.getAcceptedPlans().size() == 1);
        b1.clearAllPlans();
        Assert.assertTrue(b1.getAcceptedPlans().size() == 0);

        // test clear unfixed plan with fixed plan left in accepted plans
        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildPlan());
        Assert.assertTrue(b1.getAcceptedPlans().size() == 2);
        b1.clearAllPlans();
        Assert.assertTrue(b1.getAcceptedPlans().size() == 1);

        // test clear unaccepted plans
        b1.addUnacceptedPlan(buildPlan());
        b1.addUnacceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildFixPlan());

        Assert.assertTrue(b1.getAcceptedPlans().size() == 3);
        Assert.assertTrue(b1.getUnacceptedPlans().size() == 2);

        b1.clearAllPlans();
        Assert.assertTrue(b1.getUnacceptedPlans().size() == 0);
        Assert.assertTrue(b1.getAcceptedPlans().size() == 1);
        Assert.assertTrue(b1.getFixPlans().iterator().next().getTablesHashCode() == PlanInfo.REBUILD_PLAN_HASH_CODE);
    }

    /**
     * DDL invalidation keeps fixed plans in memory for on-demand rebuild and must also keep their metadb rows.
     * Only the ids of plans actually removed from the baseline may be returned to
     * PlanManager.deleteBaselineUnfixed(), which forwards this set to BaselineInfoAccessor.deletePlans().
     */
    @Test
    public void testClearAllPlansExcludesFixedPlanFromMetaDbDeletion() {
        BaselineInfo baselineInfo = new BaselineInfo("test sql", Collections.emptySet());
        PlanInfo fixedPlan = buildFixPlan();
        PlanInfo acceptedPlan = buildPlan();
        PlanInfo unacceptedPlan = buildPlan();

        baselineInfo.addAcceptedPlan(fixedPlan);
        baselineInfo.addAcceptedPlan(acceptedPlan);
        baselineInfo.addUnacceptedPlan(unacceptedPlan);

        Set<Integer> removePlanIds = baselineInfo.clearAllPlans();

        Assert.assertTrue(baselineInfo.getAcceptedPlans().containsKey(fixedPlan.getId()));
        Assert.assertTrue(fixedPlan.getTablesHashCode() == PlanInfo.REBUILD_PLAN_HASH_CODE);
        Assert.assertTrue(!baselineInfo.getAcceptedPlans().containsKey(acceptedPlan.getId()));
        Assert.assertTrue(baselineInfo.getUnacceptedPlans().isEmpty());
        Assert.assertTrue(!removePlanIds.contains(fixedPlan.getId()));
        Assert.assertTrue(removePlanIds.contains(acceptedPlan.getId()));
        Assert.assertTrue(removePlanIds.contains(unacceptedPlan.getId()));
        Assert.assertTrue(removePlanIds.size() == 2);
    }

    @Test
    public void testMergeFixPlanExceedMaxPlanSize() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.merge("test", b1);
        System.out.println(b2.getAcceptedPlans().size());
        Assert.assertTrue(b2.getFixPlans().size() == 12 && b2.getAcceptedPlans().size() == 12);
    }

    @Test
    public void testMergeHotEvolution() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        b1.setHotEvolution(true);
        b2.merge("test", b1);
        Assert.assertTrue(b2.isHotEvolution());

        b1 = new BaselineInfo("test sql", Collections.emptySet());
        b2 = new BaselineInfo("test sql", Collections.emptySet());

        b2.setHotEvolution(true);
        b2.merge("test", b1);
        Assert.assertTrue(b2.isHotEvolution());

        b1 = new BaselineInfo("test sql", Collections.emptySet());
        b2 = new BaselineInfo("test sql", Collections.emptySet());

        b2.merge("test", b1);
        Assert.assertTrue(!b2.isHotEvolution());
    }

    @Test
    public void testMergeExpiredPlan() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        b1.addAcceptedPlan(buildFixPlan());
        b1.addAcceptedPlan(buildExpiredPlan());
        b1.addAcceptedPlan(buildExpiredPlan());
        b1.addAcceptedPlan(buildPlan());
        b1.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildFixPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildExpiredPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.addAcceptedPlan(buildPlan());
        b2.merge("test", b1);
        System.out.println(b2.getAcceptedPlans().size());
        Assert.assertTrue(b2.getFixPlans().size() == 2 && b2.getAcceptedPlans().size() == 6);
    }

    /**
     * DDL bumps the table version but only one CN has completed SPM_FIX_DDL_HASHCODE_UPDATE
     * for a fixed plan (planId unchanged since the rebuilt plan JSON is identical).
     * Merging the stale replica first, then the fresh replica, must still end up with the
     * fresh (current) tablesHashCode.
     */
    @Test
    public void testMergeFixedPlanHashCode_staleThenFresh_picksFresh() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        int sharedId = planId.incrementAndGet();
        int currentHash = PlanManagerUtil.computeTablesVersion(Collections.emptySet(), "test", null);
        int staleHash = currentHash + 1;

        b1.addAcceptedPlan(buildFixPlanWithHash(sharedId, staleHash));
        b2.addAcceptedPlan(buildFixPlanWithHash(sharedId, currentHash));

        b1.merge("test", b2);

        Assert.assertTrue(b1.getAcceptedPlans().get(sharedId).getTablesHashCode() == currentHash);
    }

    /**
     * Same scenario as above but with reversed merge order: the fresh replica already present,
     * the stale replica merged in. Result must be order-independent and still keep the fresh hash.
     */
    @Test
    public void testMergeFixedPlanHashCode_freshThenStale_picksFresh() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        int sharedId = planId.incrementAndGet();
        int currentHash = PlanManagerUtil.computeTablesVersion(Collections.emptySet(), "test", null);
        int staleHash = currentHash + 1;

        b1.addAcceptedPlan(buildFixPlanWithHash(sharedId, currentHash));
        b2.addAcceptedPlan(buildFixPlanWithHash(sharedId, staleHash));

        b1.merge("test", b2);

        Assert.assertTrue(b1.getAcceptedPlans().get(sharedId).getTablesHashCode() == currentHash);
    }

    /**
     * DDL happened but no CN has rebuilt the fixed plan yet (multi-CN scenario where the
     * fix SQL hasn't reached any node): every replica reports a stale hashcode. merge() must
     * not fabricate a match with the current table hash, and should leave the existing value
     * untouched so the plan is still recognized as stale and rebuilt on demand.
     */
    @Test
    public void testMergeFixedPlanHashCode_noReplicaMatches_keepsExistingValue() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        int sharedId = planId.incrementAndGet();
        int currentHash = PlanManagerUtil.computeTablesVersion(Collections.emptySet(), "test", null);
        int staleHash1 = currentHash + 1;
        int staleHash2 = currentHash + 2;

        b1.addAcceptedPlan(buildFixPlanWithHash(sharedId, staleHash1));
        b2.addAcceptedPlan(buildFixPlanWithHash(sharedId, staleHash2));

        b1.merge("test", b2);

        Assert.assertTrue(b1.getAcceptedPlans().get(sharedId).getTablesHashCode() == staleHash1);
    }

    private static PlanInfo buildFixPlanWithHash(int id, int tablesHashCode) {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, true, "", "", "", tablesHashCode, 0);
        p.setId(id);
        return p;
    }

    @Test
    public void testTableSetSerialized() {
        Set<Pair<String, String>> tableSet = Sets.newHashSet();
        tableSet.add(Pair.of(null, "xxa"));

        String json = BaselineInfo.serializeTableSet(tableSet);
        Set<Pair<String, String>> deserializedTableSet = BaselineInfo.deserializeTableSet(json);
        System.out.println(deserializedTableSet);
        Assert.assertTrue(deserializedTableSet.iterator().next().getKey() == null);
    }

    /**
     * test RebuildAtLoad baseline serialize
     */
    @Test
    public void testRebuildAtLoadBaselineHintSerialized() {
        String hint = "test hint info";
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        b1.setHint(hint);
        b1.setRebuildAtLoad(true);
        b1.setUsePostPlanner(true);
        b1.setHotEvolution(true);
        Assert.assertTrue(b1.isRebuildAtLoad());
        String json = BaselineInfo.serializeToJson(b1, false);
        BaselineInfo b2 = BaselineInfo.deserializeFromJson(json);
        Assert.assertTrue(b2.isRebuildAtLoad() && StringUtils.isNotEmpty(b2.getHint()) && b2.isUsePostPlanner());
        Assert.assertTrue(b2.isHotEvolution());
    }

    /**
     * test RebuildAtLoad baseline serialize
     */
    @Test
    public void testRebuildAtLoadBaselineHotEvolutionSerialized() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        b1.setHotEvolution(true);
        String json = BaselineInfo.serializeToJson(b1, false);
        BaselineInfo b2 = BaselineInfo.deserializeFromJson(json);
        Assert.assertTrue(b2.isHotEvolution());

        b1 = new BaselineInfo("test sql", Collections.emptySet());
        json = BaselineInfo.serializeToJson(b1, false);
        b2 = BaselineInfo.deserializeFromJson(json);
        Assert.assertTrue(!b2.isHotEvolution());
    }

    @Test
    public void testBaselineSupport() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        assert !PlanManagerUtil.baselineSupported(new DirectMultiDBTableOperation(cluster, cluster.traitSet()));
        assert !PlanManagerUtil.baselineSupported(new DirectShardingKeyTableOperation(cluster, cluster.traitSet()));
        assert !PlanManagerUtil.baselineSupported(new DirectTableOperation(cluster, cluster.traitSet()));
        assert !PlanManagerUtil.baselineSupported(new PhyDdlTableOperation(cluster, cluster.traitSet()));
        assert !PlanManagerUtil.baselineSupported(new PhyTableOperation(cluster, cluster.traitSet()));
    }

    /**
     * 当重建加载计划为空时，直接返回。
     */
    @Test
    public void testResetWhenRebuildIsNull() {
        BaselineInfo baselineInfo = new BaselineInfo("test sql", Collections.emptySet());
        assertNull(baselineInfo.getRebuildAtLoadPlan());
        baselineInfo.resetRebuildAtLoadPlanIfMismatched(123);
        assertNull(baselineInfo.getRebuildAtLoadPlan());
    }

    /**
     * 当当前哈希值与提供的哈希值匹配时，不重置重建加载计划。
     */
    @Test
    public void testResetWhenHashMatched() {
        BaselineInfo baselineInfo = new BaselineInfo("test sql", Collections.emptySet());
        PlanInfo pMock = mock(PlanInfo.class);
        when(pMock.getTablesHashCode()).thenReturn(123);
        baselineInfo.computeRebuiltAtLoadPlanIfNotExists(() -> pMock);
        assertTrue(baselineInfo.getRebuildAtLoadPlan() == pMock);
        baselineInfo.resetRebuildAtLoadPlanIfMismatched(123);
        assertTrue(baselineInfo.getRebuildAtLoadPlan() == pMock);
    }

    /**
     * 当当前哈希值与提供的哈希值不匹配时，重置重建加载计划。
     */
    @Test
    public void testResetWhenHashNotMatched() {
        BaselineInfo baselineInfo = new BaselineInfo("test sql", Collections.emptySet());
        PlanInfo pMock = mock(PlanInfo.class);
        when(pMock.getTablesHashCode()).thenReturn(122);
        baselineInfo.computeRebuiltAtLoadPlanIfNotExists(() -> pMock);
        assertTrue(baselineInfo.getRebuildAtLoadPlan() == pMock);
        baselineInfo.resetRebuildAtLoadPlanIfMismatched(123);
        assertTrue(baselineInfo.getRebuildAtLoadPlan() == null);
    }

    @Test
    public void testHotEvolution() {
        BaselineInfo baselineInfo = new BaselineInfo("test sql", Collections.emptySet());
        baselineInfo.setHotEvolution(true);
        baselineInfo.setExtend(baselineInfo.encodeExtend());
        String json = BaselineInfo.serializeToJson(baselineInfo, true);
        System.out.println(json);

        BaselineInfo baselineInfo2 = BaselineInfo.deserializeFromJson(json);

        assert baselineInfo2.isHotEvolution();
        assert BaselineInfo.hotEvolution(baselineInfo.getExtend());
        assert BaselineInfo.hotEvolution(baselineInfo2.getExtend());
    }

    /**
     * buildPlanRecord must map both null and epoch-0 lastExecuteTime (never executed,
     * 0 comes back from rs.getLong on a SQL NULL timestamp after a storage roundtrip)
     * to -1 so that persisting writes SQL NULL instead of a Timestamp(0) that metadb
     * rejects with "Incorrect datetime value".
     */
    @Test
    public void testBuildPlanRecordNeverExecutedLastExecuteTime() {
        BaselineInfo baselineInfo = new BaselineInfo("test sql", Collections.emptySet());

        PlanInfo neverExecuted =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, null, 0, 1D, 1D, true, false, "", "", "", 1, 0);
        neverExecuted.setId(planId.incrementAndGet());

        PlanInfo epochZero =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, 0L, 0, 1D, 1D, true, false, "", "", "", 1, 0);
        epochZero.setId(planId.incrementAndGet());

        long executedTime = System.currentTimeMillis() / 1000;
        PlanInfo executed =
            new PlanInfo(1, "", executedTime, executedTime, 1, 1D, 1D, true, false, "", "", "", 1, 0);
        executed.setId(planId.incrementAndGet());

        baselineInfo.addAcceptedPlan(neverExecuted);
        baselineInfo.addAcceptedPlan(epochZero);
        baselineInfo.addAcceptedPlan(executed);

        java.util.List<BaselineInfoRecord> records = baselineInfo.buildPlanRecord("test", "inst");
        Assert.assertTrue(records.size() == 3);
        for (BaselineInfoRecord record : records) {
            if (record.getPlanId() == neverExecuted.getId() || record.getPlanId() == epochZero.getId()) {
                Assert.assertTrue(record.getLastExecuteTime() == -1);
            } else {
                Assert.assertTrue(record.getPlanId() == executed.getId());
                Assert.assertTrue(record.getLastExecuteTime() == executedTime);
            }
        }
    }

    /**
     * merge must keep unmaterialized accepted plans (getPlan(null, null) == null) that
     * were never executed (lastExecuteTime null in memory, 0 after a storage roundtrip):
     * "not materialized" is not "unsupported" and "never executed" is not "expired".
     */
    @Test
    public void testMergeKeepsUnmaterializedNeverExecutedPlan() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        PlanInfo nullTime =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, null, 0, 1D, 1D, true, false, "", "", "", 1, 0);
        nullTime.setId(planId.incrementAndGet());
        PlanInfo zeroTime =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, 0L, 0, 1D, 1D, true, false, "", "", "", 1, 0);
        zeroTime.setId(planId.incrementAndGet());

        b2.addAcceptedPlan(nullTime);
        b2.addAcceptedPlan(zeroTime);
        b2.merge("test", b1);

        Assert.assertTrue(b2.getAcceptedPlans().containsKey(nullTime.getId()));
        Assert.assertTrue(b2.getAcceptedPlans().containsKey(zeroTime.getId()));
    }

    /**
     * merge must remove a never-executed unmaterialized accepted plan whose createTime is
     * already beyond SPM_RECENTLY_EXECUTED_PERIOD: "never executed" falls back to createTime,
     * so stale never-executed baselines still expire instead of living forever.
     */
    @Test
    public void testMergeRemovesNeverExecutedPlanWithExpiredCreateTime() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        long expiredCreateTime =
            (System.currentTimeMillis() - InstConfUtil.getLong(SPM_RECENTLY_EXECUTED_PERIOD) - 1000) / 1000;
        PlanInfo nullTime = new PlanInfo(1, "", expiredCreateTime, null, 0, 1D, 1D, true, false, "", "", "", 1, 0);
        nullTime.setId(planId.incrementAndGet());
        PlanInfo zeroTime = new PlanInfo(1, "", expiredCreateTime, 0L, 0, 1D, 1D, true, false, "", "", "", 1, 0);
        zeroTime.setId(planId.incrementAndGet());

        b2.addAcceptedPlan(nullTime);
        b2.addAcceptedPlan(zeroTime);
        b2.merge("test", b1);

        Assert.assertTrue(!b2.getAcceptedPlans().containsKey(nullTime.getId()));
        Assert.assertTrue(!b2.getAcceptedPlans().containsKey(zeroTime.getId()));
    }

    /**
     * merge must conservatively keep a never-executed unmaterialized accepted plan when both
     * lastExecuteTime and createTime are invalid (<= 0): dropping a user baseline on corrupt
     * timestamps is worse than keeping it. Must not throw on such plans either.
     */
    @Test
    public void testMergeKeepsNeverExecutedPlanWithInvalidTimestamps() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        PlanInfo invalidTimes = new PlanInfo(1, "", 0L, null, 0, 1D, 1D, true, false, "", "", "", 1, 0);
        invalidTimes.setId(planId.incrementAndGet());

        b2.addAcceptedPlan(invalidTimes);
        b2.merge("test", b1);

        Assert.assertTrue(b2.getAcceptedPlans().containsKey(invalidTimes.getId()));
    }

    /**
     * merge must still remove a materialized plan whose RelNode is not baseline-supported.
     */
    @Test
    public void testMergeRemovesUnsupportedMaterializedPlan() {
        BaselineInfo b1 = new BaselineInfo("test sql", Collections.emptySet());
        BaselineInfo b2 = new BaselineInfo("test sql", Collections.emptySet());

        PlanInfo unsupported = buildPlan();
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        unsupported.resetPlan(new DirectTableOperation(cluster, cluster.traitSet()), new ExecutionContext());

        b2.addAcceptedPlan(unsupported);
        b2.merge("test", b1);

        Assert.assertTrue(!b2.getAcceptedPlans().containsKey(unsupported.getId()));
    }

    private static PlanInfo buildFixPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, true, "", "", "", 1, 0);
        p.setId(planId.incrementAndGet());
        return p;
    }

    private static PlanInfo buildPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, false, "", "", "", 1, 0);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        p.resetPlan(new Gather(cluster, cluster.traitSet(), null), new ExecutionContext());
        p.setId(planId.incrementAndGet());
        return p;
    }

    private static PlanInfo buildExpiredPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000,
                (System.currentTimeMillis() - InstConfUtil.getLong(SPM_RECENTLY_EXECUTED_PERIOD) - 1000) / 1000, 0, 1D,
                1D,
                true, false, "", "", "", 1, 0);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        p.resetPlan(new Gather(cluster, cluster.traitSet(), null), new ExecutionContext());
        p.setId(planId.incrementAndGet());
        return p;
    }

    /**
     * build baseline with specified sql
     * baseline returned contains fixed/unfixed/expired plans in accepted plans
     */
    public static BaselineInfo buildBaselineInfoWithFixedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addAcceptedPlan(buildFixPlan());
        b.addAcceptedPlan(buildPlan());
        b.addAcceptedPlan(buildExpiredPlan());

        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }

    /**
     * build baseline with specified sql
     * baseline returned contains unfixed/expired plans in accepted plans
     */
    public static BaselineInfo buildBaselineInfoWithoutFixedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addAcceptedPlan(buildPlan());
        b.addAcceptedPlan(buildExpiredPlan());

        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }

    /**
     * build baseline with specified sql
     * baseline returned had empty accepted plan list
     */
    public static BaselineInfo buildBaselineInfoWithEmptyAcceptedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }

    public static BaselineInfo buildRebuildAtLoadBaseline(String hint, String sql) {
        BaselineInfo b1 = new BaselineInfo(sql, Collections.emptySet());
        b1.setHint(hint);
        b1.setRebuildAtLoad(true);
        b1.setUsePostPlanner(true);
        b1.setHotEvolution(true);
        return b1;
    }

    public static BaselineInfo buildHotGsiBaseline(String sql) {
        BaselineInfo b1 = new BaselineInfo(sql, Collections.emptySet());
        b1.setRebuildAtLoad(false);
        b1.setUsePostPlanner(true);
        b1.setHotEvolution(true);
        b1.addAcceptedPlan(buildPlan());
        return b1;
    }

    /**
     * test serializeToJsonForShow method
     */
    @Test
    public void testSerializeToJsonForShow() {
        // create baseline with table set
        Set<Pair<String, String>> tableSet = Sets.newHashSet();
        tableSet.add(Pair.of("test_schema", "test_table"));
        tableSet.add(Pair.of(null, "another_table"));

        BaselineInfo baselineInfo = new BaselineInfo("SELECT * FROM test_table WHERE id = ?", tableSet);

        // add accepted plans
        PlanInfo plan1 = buildPlan();
        PlanInfo plan2 = buildFixPlan();
        baselineInfo.addAcceptedPlan(plan1);
        baselineInfo.addAcceptedPlan(plan2);

        // set extend info
        baselineInfo.setHint("/*+ test hint */");
        baselineInfo.setHotEvolution(true);
        baselineInfo.setDirty(true);

        // serialize to json
        String json = BaselineInfo.serializeToJsonForShow(baselineInfo);

        // verify json contains expected fields
        Assert.assertTrue(StringUtils.isNotEmpty(json));
        Assert.assertTrue(json.contains("\"id\""));
        Assert.assertTrue(json.contains("\"parameterSql\""));
        Assert.assertTrue(json.contains("SELECT * FROM test_table WHERE id = ?"));
        Assert.assertTrue(json.contains("\"tableSet\""));
        Assert.assertTrue(json.contains("test_table"));
        Assert.assertTrue(json.contains("\"acceptedPlans\""));
        Assert.assertTrue(json.contains("\"extend\""));
        Assert.assertTrue(json.contains("\"dirty\""));

        // verify accepted plans are included
        Assert.assertTrue(json.contains(String.valueOf(plan1.getId())));
        Assert.assertTrue(json.contains(String.valueOf(plan2.getId())));

        System.out.println("Serialized JSON: " + json);
    }

    /**
     * test grayStatus method
     */
    @Test
    public void testGrayStatus() {
        BaselineInfo baselineInfo = new BaselineInfo("SELECT * FROM test", Collections.emptySet());

        // test empty baseline - should return false
        Assert.assertTrue(!baselineInfo.grayStatus());

        // test with non-gray plans (grayPercentage = 0)
        PlanInfo plan1 = buildPlan();
        plan1.setGrayPercentage(0);
        baselineInfo.addAcceptedPlan(plan1);
        Assert.assertTrue(!baselineInfo.grayStatus());

        // test with non-gray plans (grayPercentage = 100)
        PlanInfo plan2 = buildPlan();
        plan2.setGrayPercentage(100);
        baselineInfo.addAcceptedPlan(plan2);
        Assert.assertTrue(!baselineInfo.grayStatus());

        // test with one gray plan (grayPercentage = 50)
        PlanInfo grayPlan = buildGrayPlan(50);
        baselineInfo.addAcceptedPlan(grayPlan);
        Assert.assertTrue(baselineInfo.grayStatus());

        // test baseline with only gray plans
        BaselineInfo baselineInfo2 = new BaselineInfo("SELECT * FROM test2", Collections.emptySet());
        PlanInfo grayPlan1 = buildGrayPlan(30);
        PlanInfo grayPlan2 = buildGrayPlan(70);
        baselineInfo2.addAcceptedPlan(grayPlan1);
        baselineInfo2.addAcceptedPlan(grayPlan2);
        Assert.assertTrue(baselineInfo2.grayStatus());

        // test baseline with mixed plans
        BaselineInfo baselineInfo3 = new BaselineInfo("SELECT * FROM test3", Collections.emptySet());
        baselineInfo3.addAcceptedPlan(buildPlan());
        baselineInfo3.addAcceptedPlan(buildFixPlan());
        Assert.assertTrue(!baselineInfo3.grayStatus());
        baselineInfo3.addAcceptedPlan(buildGrayPlan(10));
        Assert.assertTrue(baselineInfo3.grayStatus());
    }

    private static PlanInfo buildGrayPlan(int grayPercentage) {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, false, "", "", "", 1, 0);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        p.resetPlan(new Gather(cluster, cluster.traitSet(), null), new ExecutionContext());
        p.setId(planId.incrementAndGet());
        p.setGrayPercentage(grayPercentage);
        return p;
    }
}

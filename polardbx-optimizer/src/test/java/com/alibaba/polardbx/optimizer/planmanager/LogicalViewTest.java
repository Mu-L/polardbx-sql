package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * logical view ut case
 *
 * @author fangwu
 */
public class LogicalViewTest extends BaseRuleTest {

    @Test
    public void testLogicalViewColumnOriginsCopy() {
        LogicalTableScan scan1 = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView lv1 = LogicalView.create(scan1, scan1.getTable());

        LogicalTableScan scan2 = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView lv2 = LogicalView.create(scan1, scan1.getTable());

        List<RexNode> rexNodeList = Lists.newArrayList();
        final RelDataTypeFactory typeFactory = relOptCluster.getTypeFactory();
        RelDataType relDataType = typeFactory.createJoinType(scan1.getRowType(), scan2.getRowType());
        rexNodeList.add(relOptCluster.getRexBuilder().makeInputRef(relDataType, 1));
        rexNodeList.add(relOptCluster.getRexBuilder().makeInputRef(relDataType, 5));

        BKAJoin bkaJoin = BKAJoin.create(scan1.getTraitSet().replace(DrdsConvention.INSTANCE), lv1, lv2,
            relOptCluster.getRexBuilder().makeCall(
                SqlStdOperatorTable.EQUALS, rexNodeList), ImmutableSet.of(), JoinRelType.INNER, false,
            ImmutableList.of(), null);
        lv1.setLookupInfo(bkaJoin);
        lv1.optimize();
        int size = lv1.getLookupInfo().getAllJoinKeys().size();
        Assert.assertTrue(size > 0);

        LogicalView b = (LogicalView) lv1.clone();
        int checkSize = b.getLookupInfo().getAllJoinKeys().size();
        Assert.assertTrue(size == checkSize);

        // test index scan
        LogicalIndexScan logicalIndexScan = new LogicalIndexScan(b);
        checkSize = logicalIndexScan.getLookupInfo().getAllJoinKeys().size();
        Assert.assertTrue(size == checkSize);

        LogicalIndexScan logicalIndexScanCopy = logicalIndexScan.copy(logicalIndexScan.getTraitSet());

        checkSize = logicalIndexScanCopy.getLookupInfo().getAllJoinKeys().size();
        Assert.assertTrue(size == checkSize);

        // test LogicalModifyView
        LogicalModifyView logicalModifyView = new LogicalModifyView(b);
        checkSize = logicalModifyView.getLookupInfo().getAllJoinKeys().size();
        Assert.assertTrue(size == checkSize);
        LogicalModifyView logicalModifyViewCopy = logicalModifyView.copy(logicalModifyView.getTraitSet());

        checkSize = logicalModifyViewCopy.getLookupInfo().getAllJoinKeys().size();
        Assert.assertTrue(size == checkSize);

        // test oss table scan
        OSSTableScan ossTableScan = new OSSTableScan(b);
        checkSize = ossTableScan.getLookupInfo().getAllJoinKeys().size();
        Assert.assertTrue(size == checkSize);
        OSSTableScan ossTableScanCopy = (OSSTableScan) ossTableScan.copy(ossTableScan.getTraitSet());

        checkSize = ossTableScanCopy.getLookupInfo().getAllJoinKeys().size();
        Assert.assertTrue(size == checkSize);
    }

    @Test
    public void testFlashBack() {
        LogicalTableScan scan1 = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        org.junit.Assert.assertFalse(RelUtils.containFlashback(scan1));

        scan1.setFlashback(relOptCluster.getRexBuilder().makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP));
        org.junit.Assert.assertTrue(RelUtils.containFlashback(scan1));
    }

    @Test
    public void testIsSingleGroupMethods() {
        // Create a LogicalView instance
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView logicalView = LogicalView.create(scan, scan.getTable());

        // Test that isSingleGroupForExecutor returns the same value as isSingleGroup
        // Initially both should return false
        boolean isSingleGroup = logicalView.isSingleGroup();
        boolean isSingleGroupForExecutor = logicalView.isSingleGroupForExecutor();

        org.junit.Assert.assertEquals("isSingleGroupForExecutor should match isSingleGroup",
            isSingleGroup, isSingleGroupForExecutor);

        // Note: To test with different values, we would need to mock or set the internal isSingleGroup field,
        // which is not easily accessible. In real usage, the value would be set during construction or optimization.
    }

    @Test
    public void testOSSTableScanIsSingleGroupMethods() {
        // Create a LogicalView instance
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView logicalView = LogicalView.create(scan, scan.getTable());

        // Create an OSSTableScan from the LogicalView
        OSSTableScan ossTableScan = new OSSTableScan(logicalView);

        // Test that isSingleGroupForExecutor works on OSSTableScan
        // Initially both should return false
        boolean isSingleGroup = ossTableScan.isSingleGroup();
        boolean isSingleGroupForExecutor = ossTableScan.isSingleGroupForExecutor();

        org.junit.Assert.assertEquals("isSingleGroupForExecutor should match isSingleGroup in OSSTableScan",
            isSingleGroup, isSingleGroupForExecutor);
    }

    @Test
    public void testLogicalIndexScanClone() {
        // Create a LogicalTableScan as the base
        LogicalTableScan scan1 = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView lv1 = LogicalView.create(scan1, scan1.getTable());

        // Create LogicalIndexScan from LogicalView
        LogicalIndexScan originalIndexScan = new LogicalIndexScan(lv1);

        // Set some properties to test cloning
        originalIndexScan.setProjectSwitched(true);
        originalIndexScan.setScalarList(Lists.newArrayList());

        // Test clone method
        LogicalIndexScan clonedIndexScan = (LogicalIndexScan) originalIndexScan.clone();

        // Verify that clone is not the same instance
        Assert.assertTrue(originalIndexScan != clonedIndexScan);

        // Verify that basic properties are copied correctly
        Assert.assertTrue(clonedIndexScan.isProjectSwitched() == originalIndexScan.isProjectSwitched());
        Assert.assertTrue(clonedIndexScan.getMainTableName() == originalIndexScan.getMainTableName());

        // Verify that scalarList is copied
        Assert.assertTrue(clonedIndexScan.getScalarList() != null);
        Assert.assertTrue(clonedIndexScan.getScalarList().size() == originalIndexScan.getScalarList().size());

        // Verify that the cloned object has the same table reference
        Assert.assertTrue(clonedIndexScan.getTable() == originalIndexScan.getTable());

        // Verify that the cloned object has the same cluster reference
        Assert.assertTrue(clonedIndexScan.getCluster() == originalIndexScan.getCluster());

        // Verify that the cloned object has the same trait set
        Assert.assertTrue(clonedIndexScan.getTraitSet() != null);

        // Test that modifications to original don't affect clone
        originalIndexScan.setProjectSwitched(false);
        Assert.assertTrue(clonedIndexScan.isProjectSwitched() == true); // Should still be true

        // Test clone with scalar list modifications
        List<RexDynamicParam> newScalarList = Lists.newArrayList();
        originalIndexScan.setScalarList(newScalarList);
        // The cloned version should still have the original empty list
        Assert.assertTrue(clonedIndexScan.getScalarList().size() == 0);

        // Test that clone preserves the class type
        Assert.assertTrue(clonedIndexScan instanceof LogicalIndexScan);
        Assert.assertTrue(clonedIndexScan.getClass() == LogicalIndexScan.class);
    }
}
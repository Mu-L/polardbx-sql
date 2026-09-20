package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTblWithPhyTblVisitor;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.gms.metadb.MetaDbDataSource.DEFAULT_META_DB_GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class PlannerTest {

    @Test
    public void testEnableDirectPlanFalse() {
        PlannerContext pc = new PlannerContext();
        pc.getParamManager().getProps().put(ConnectionProperties.ENABLE_DIRECT_PLAN, "false");
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }

    @Test
    public void testDnHint() {
        PlannerContext pc = new PlannerContext();
        pc.getParamManager().getProps().put(ConnectionProperties.DN_HINT, "test hint");
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }

    /**
     * cmdBean为空的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_CmdBeanIsNull() {
        assertTrue(!Planner.isDirectHintWithGroupName("schema", null, Maps.newHashMap()));
    }

    /**
     * cmdBean不包含JSON提示的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_CmdBeanNotJsonHint() {
        HintCmdOperator.CmdBean cmdBean = mock(HintCmdOperator.CmdBean.class);
        when(cmdBean.jsonHint()).thenReturn(false);
        assertFalse(Planner.isDirectHintWithGroupName("schema", cmdBean, Maps.newHashMap()));
    }

    /**
     * 直接路由条件的数据库ID为partition name的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_DbIdIsNull() {
        String schema = "test_schema";
        HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(schema, Maps.newHashMap(), "");
        cmdBean.setJson(
            "{'extra':{'MERGE_UNION':'false'},'type':'direct','vtab':'t_school_note','dbid':'p1','realtabs':['t_school_note_6pDG_00001']}");
        assertTrue(!Planner.isDirectHintWithGroupName("schema", cmdBean, Maps.newHashMap()));
    }

    /**
     * 数据库ID包含"GROUP"的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_DbIdContainsGroup() {
        String schema = "test_schema";
        HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(schema, Maps.newHashMap(), "");
        cmdBean.setJson(
            "{'extra':{'MERGE_UNION':'false'},'type':'direct','vtab':'t_school_note','dbid':'xx_group','realtabs':['t_school_note_6pDG_00001']}");
        assertTrue(Planner.isDirectHintWithGroupName("schema", cmdBean, Maps.newHashMap()));
    }

    /**
     * 数据库ID等于 DEFAULT_META_DB_GROUP_NAME 的情况
     */
    @Test
    public void testIsDirectHintWithGroupName_DbIdEqualsDefaultMetaDbGroupName() {
        String schema = "test_schema";
        HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(schema, Maps.newHashMap(), "");
        cmdBean.setJson("{'extra':{'MERGE_UNION':'false'},'type':'direct','vtab':'t_school_note','dbid':'"
            + DEFAULT_META_DB_GROUP_NAME + "','realtabs':['t_school_note_6pDG_00001']}");
        assertTrue(Planner.isDirectHintWithGroupName("schema", cmdBean, Maps.newHashMap()));
    }

    @Test
    public void testForceIndexHint() {
        PlannerContext pc = new PlannerContext();
        pc.setLocalIndexHint(true);
        ExecutionPlan.DirectMode mode = new Planner().shouldDirectByTable(null, null, pc, null);

        assertEquals(ExecutionPlan.DirectMode.NONE, mode);
    }

    @Test
    public void testCTEPushDown() {
        SqlNode ast = mock(SqlNode.class);
        PlannerContext pc = new PlannerContext();

        try (MockedStatic<PlanManagerUtil> mockedStatic = mockStatic(PlanManagerUtil.class)) {
            mockedStatic.when(() -> PlanManagerUtil.containsDuplicateCol(ast)).thenReturn(true);
            when(ast.getKind()).thenReturn(SqlKind.UPDATE);
            try {
                Planner.getInstance().getPlan(ast, pc);
            } catch (Exception e) {
                //ignore
            }
            assertTrue(pc.getExtraCmds().get(ConnectionProperties.ENABLE_DIRECT_PLAN) == null);
            assertTrue(pc.getExtraCmds().get(ConnectionProperties.ENABLE_POST_PLANNER) == null);

            when(ast.getKind()).thenReturn(SqlKind.WITH);
            try {
                Planner.getInstance().getPlan(ast, pc);
            } catch (Exception e) {
                //ignore
            }
            assertFalse((Boolean) pc.getExtraCmds().get(ConnectionProperties.ENABLE_DIRECT_PLAN));
            assertFalse((Boolean) pc.getExtraCmds().get(ConnectionProperties.ENABLE_POST_PLANNER));
        }
    }

    // 新增：当 schema 使用物理库配置时应直接返回（无异常），即便两个 groupId 不一致
    @Test
    public void testCheckGroupIdTheSame_SkipWhenUsePhyDbConfigs() {
        try (MockedStatic<PlannerUtils> mocked = Mockito.mockStatic(PlannerUtils.class)) {
            mocked.when(() -> PlannerUtils.checkIfUseSchemaUsePhyDbConfigs("schema_skip"))
                .thenReturn(true);
            ReplaceTblWithPhyTblVisitor.checkGroupIdTheSame("schema_skip", 1L, 2L);
        }
    }

    // 新增：不使用物理库配置，且两个 groupId 一致时不应抛异常
    @Test
    public void testCheckGroupIdTheSame_EqualIds_NoThrow() {
        try (MockedStatic<PlannerUtils> mocked = Mockito.mockStatic(PlannerUtils.class)) {
            mocked.when(() -> PlannerUtils.checkIfUseSchemaUsePhyDbConfigs("schema_equal"))
                .thenReturn(false);
            ReplaceTblWithPhyTblVisitor.checkGroupIdTheSame("schema_equal", 100L, 100L);
        }
    }

    // 新增：不使用物理库配置，且两个 groupId 不一致时应抛出 TddlRuntimeException
    @Test(expected = TddlRuntimeException.class)
    public void testCheckGroupIdTheSame_Mismatch_Throws() {
        try (MockedStatic<PlannerUtils> mocked = Mockito.mockStatic(PlannerUtils.class)) {
            mocked.when(() -> PlannerUtils.checkIfUseSchemaUsePhyDbConfigs("schema_mismatch"))
                .thenReturn(false);
            ReplaceTblWithPhyTblVisitor.checkGroupIdTheSame("schema_mismatch", 1L, 2L);
        }
    }
}
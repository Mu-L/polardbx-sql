package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class PlannerConvertGroupByToOrdinalsTest {

    private SqlNode convertGroupByToOrdinals(SqlNode sqlNode) {
        Method method;
        try {
            method = Planner.class.getDeclaredMethod("convertGroupByToOrdinals", SqlNode.class);
            method.setAccessible(true);
            return (SqlNode) method.invoke(new Planner(), sqlNode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SqlSelect parseSelect(String sql) {
        SqlNodeList astList = new FastsqlParser().parse(sql);
        return (SqlSelect) astList.get(0);
    }

    /**
     * FastsqlParser rejects a subquery written literally in GROUP BY, so a
     * SqlNode containing a subquery can only appear there the way the
     * validator's alias-expansion produces it: by re-using the SELECT list
     * item's SqlNode instance and injecting it into the GROUP BY list.
     */
    private void injectSubQueryIntoGroupBy(SqlSelect select, int selectListIndex, int groupByIndex) {
        SqlNode subQueryExpr = SqlUtil.stripAs(select.getSelectList().get(selectListIndex));
        List<SqlNode> newGroupList = new ArrayList<>(select.getGroup().getList());
        newGroupList.set(groupByIndex, subQueryExpr);
        select.setGroupBy(new SqlNodeList(newGroupList, select.getGroup().getParserPosition()));
    }

    /**
     * GROUP BY item whose expression matches a SELECT list item containing a
     * subquery should be rewritten to the item's 1-based ordinal.
     */
    @Test
    public void test_groupByItem_matchingSubQuerySelectItem_convertedToOrdinal() {
        SqlSelect select = parseSelect(
            "select pk, (select max(b.x) from t2 b where b.pk = t1.pk) as sub, count(*) as cnt "
                + "from t1 group by pk, sub, cnt");
        injectSubQueryIntoGroupBy(select, 1, 1);

        SqlNode result = convertGroupByToOrdinals(select);

        Assert.assertSame(select, result);
        SqlNodeList group = ((SqlSelect) result).getGroup();
        Assert.assertEquals(3, group.size());
        Assert.assertEquals("pk", group.get(0).toString());
        Assert.assertEquals("2", group.get(1).toString());
        Assert.assertEquals("cnt", group.get(2).toString());
    }

    /**
     * When two SELECT items contain structurally identical subqueries under
     * different aliases (sub1, sub2), the GROUP BY item injected by identity
     * with the SECOND occurrence's SqlNode must resolve to that occurrence's
     * ordinal, not the first structurally-equal one. This guards against a
     * pure equalsDeep match picking the wrong ordinal and silently changing
     * grouping semantics.
     */
    @Test
    public void test_groupByItem_duplicateSubQueries_matchesByIdentityNotFirstEquals() {
        SqlSelect select = parseSelect(
            "select pk, (select max(b.x) from t2 b where b.pk = t1.pk) as sub1, "
                + "(select max(b.x) from t2 b where b.pk = t1.pk) as sub2 "
                + "from t1 group by pk, sub2");
        SqlNode secondSubQuery = SqlUtil.stripAs(select.getSelectList().get(2));
        List<SqlNode> newGroupList = new ArrayList<>(select.getGroup().getList());
        newGroupList.set(1, secondSubQuery);
        select.setGroupBy(new SqlNodeList(newGroupList, select.getGroup().getParserPosition()));

        SqlNode result = convertGroupByToOrdinals(select);

        SqlNodeList group = ((SqlSelect) result).getGroup();
        Assert.assertEquals("pk", group.get(0).toString());
        Assert.assertEquals("3", group.get(1).toString());
    }

    /**
     * GROUP BY without any subquery item must be left untouched.
     */
    @Test
    public void test_groupByWithoutSubQuery_unchanged() {
        SqlSelect select = parseSelect("select a, b from t1 group by a, b");

        SqlNode result = convertGroupByToOrdinals(select);

        SqlNodeList group = ((SqlSelect) result).getGroup();
        Assert.assertEquals("a", group.get(0).toString());
        Assert.assertEquals("b", group.get(1).toString());
    }

    /**
     * A SELECT with no GROUP BY clause should be returned unchanged.
     */
    @Test
    public void test_noGroupBy_returnsSameNode() {
        SqlSelect select = parseSelect("select a from t1");

        SqlNode result = convertGroupByToOrdinals(select);

        Assert.assertSame(select, result);
        Assert.assertNull(((SqlSelect) result).getGroup());
    }

    /**
     * A subquery GROUP BY item with no matching SELECT list item must be left
     * as-is (no ordinal substitution when there is nothing to reference).
     */
    @Test
    public void test_groupBySubQuery_withoutMatchingSelectItem_unchanged() {
        SqlSelect select = parseSelect("select pk, sub, cnt from t1 group by pk, other, cnt");
        SqlSelect otherSelect = parseSelect(
            "select (select max(b.x) from t2 b where b.pk = t1.pk) as sub from t1");
        SqlNode unmatchedSubQuery = SqlUtil.stripAs(otherSelect.getSelectList().get(0));
        List<SqlNode> newGroupList = new ArrayList<>(select.getGroup().getList());
        newGroupList.set(1, unmatchedSubQuery);
        select.setGroupBy(new SqlNodeList(newGroupList, select.getGroup().getParserPosition()));

        SqlNode result = convertGroupByToOrdinals(select);

        SqlNodeList group = ((SqlSelect) result).getGroup();
        Assert.assertEquals("pk", group.get(0).toString());
        Assert.assertTrue(group.get(1).toString().toLowerCase().contains("select"));
    }

    /**
     * Non-SqlSelect nodes must be returned unchanged.
     */
    @Test
    public void test_nonSqlSelectNode_returnedUnchanged() {
        SqlNode nonSelect = org.mockito.Mockito.mock(SqlNode.class);

        SqlNode result = convertGroupByToOrdinals(nonSelect);

        Assert.assertSame(nonSelect, result);
    }
}

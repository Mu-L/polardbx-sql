
package com.alibaba.polardbx.gms.metadb;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class RoutingRuleRecordTest {

    static class TestRoutingRuleRecord extends RoutingRuleRecord {
        public TestRoutingRuleRecord() {
            super();
        }

        @Override
        protected Map<Integer, ParameterContext> buildInsertParams() {
            return super.buildInsertParams();
        }
    }

    private ResultSet mockResultSet;

    private TestRoutingRuleRecord routingRuleRecord;

    @Before
    public void setUp() {
        routingRuleRecord = new TestRoutingRuleRecord();
        mockResultSet = mock(ResultSet.class);
    }

    @Test
    public void fill_AllFieldsPresent_FillsCorrectly() throws SQLException {
        Mockito.when(mockResultSet.getLong("id")).thenReturn(1L);
        Mockito.when(mockResultSet.getString("rule_name")).thenReturn("rule1");
        Mockito.when(mockResultSet.getString("user_name")).thenReturn("user1");
        Mockito.when(mockResultSet.getString("template_id")).thenReturn("template1");
        Mockito.when(mockResultSet.getString("keywords")).thenReturn("[\"keyword1\", \"keyword2\"]");
        Mockito.when(mockResultSet.getString("routing_type")).thenReturn("type1");

        RoutingRuleRecord result = routingRuleRecord.fill(mockResultSet);

        Assert.assertEquals(1L, result.id);
        Assert.assertEquals("rule1", result.ruleName);
        Assert.assertEquals("user1", result.userName);
        Assert.assertEquals("template1", result.templateId);
        Assert.assertEquals(Arrays.asList("keyword1", "keyword2"), result.keywords);
        Assert.assertEquals("type1", result.routingType);
    }

    @Test
    public void buildInsertParams_AllFieldsSet_CorrectParameters() {
        // 准备
        routingRuleRecord.instId = "inst1";
        routingRuleRecord.ruleName = "rule1";
        routingRuleRecord.userName = "user1";
        routingRuleRecord.templateId = "template1";
        List<String> keywords = new ArrayList<>();
        keywords.add("keyword1");
        keywords.add("keyword2");
        routingRuleRecord.keywords = keywords;
        routingRuleRecord.routingType = "type1";

        Map<Integer, ParameterContext> params = routingRuleRecord.buildInsertParams();

        Assert.assertEquals(6, params.size());
        Assert.assertEquals("inst1", params.get(1).getValue());
        Assert.assertEquals("rule1", params.get(2).getValue());
        Assert.assertEquals("user1", params.get(3).getValue());
        Assert.assertEquals("template1", params.get(4).getValue());
        Assert.assertEquals("[\"keyword1\",\"keyword2\"]", params.get(5).getValue());
        Assert.assertEquals("type1", params.get(6).getValue());
    }
}
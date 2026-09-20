package com.alibaba.polardbx.ccl;

import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.impl.CclService;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.google.common.collect.Lists;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.mockito.Mockito.when;

/**
 * Unit tests covering the identifier normalization in {@link CclService} when matching the
 * db / table name of a CCL rule.
 *
 * <p>The db / table name coming from a {@link PrivilegeVerifyItem} is normalized via
 * {@code SQLUtils.normalizeNoTrim} on the {@link CclService} consumer side before being compared
 * with the configured ccl rule, so that quoted identifiers (e.g. {@code `test`}) match the same
 * rule as their unquoted counterparts.</p>
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class CclServiceBacktickTest {

    private static final String USER = "user";
    private static final String HOST = "192.168.0.10";
    private static final String DB_NAME = "test";
    private static final String TABLE_NAME = "t1";

    @Mock
    private ICclConfigService mockCclConfigService;

    @InjectMocks
    private CclService target;

    /**
     * Regression: schema name without backticks must still match exactly as before.
     */
    @Test
    public void testSchemaNameWithoutBacktickStillMatches() {
        givenRules(buildRule("ruleSchemaPlain", DB_NAME, "*"));

        ExecutionContext ec = buildExecutionContext(DB_NAME, Lists.newArrayList());

        assertMatched(ec, "ruleSchemaPlain");
    }

    /**
     * Scenario: db name from the privilege item carries backticks and matches a rule whose
     * dbName is {@code test}. Matching goes through the privilege-item branch.
     */
    @Test
    public void testDbNameWithBacktickMatches() {
        givenRules(buildRule("ruleDbBacktick", DB_NAME, "*"));

        PrivilegeVerifyItem item = buildPrivilegeItem("`" + DB_NAME + "`", TABLE_NAME);
        ExecutionContext ec = buildExecutionContext(DB_NAME, Lists.newArrayList(item));

        assertMatched(ec, "ruleDbBacktick");
    }

    /**
     * Regression: db name without backticks in the privilege item must still match.
     */
    @Test
    public void testDbNameWithoutBacktickStillMatches() {
        givenRules(buildRule("ruleDbPlain", DB_NAME, "*"));

        PrivilegeVerifyItem item = buildPrivilegeItem(DB_NAME, TABLE_NAME);
        ExecutionContext ec = buildExecutionContext(DB_NAME, Lists.newArrayList(item));

        assertMatched(ec, "ruleDbPlain");
    }

    /**
     * Scenario: when the privilege item has no db, the schema from the privilege context is used
     * as a fallback. With a plain (unquoted) schema the fallback must still match the rule.
     */
    @Test
    public void testDbNameFallbackToSchemaMatches() {
        givenRules(buildRule("ruleDbFallback", DB_NAME, "*"));

        PrivilegeVerifyItem item = buildPrivilegeItem("", TABLE_NAME);
        ExecutionContext ec = buildExecutionContext(DB_NAME, Lists.newArrayList(item));

        assertMatched(ec, "ruleDbFallback");
    }

    /**
     * Scenario: db name from the privilege item carries an escaped backtick (e.g. {@code `my``db`}).
     * It must be normalized to {@code my`db} (one literal backtick) and match the configured rule.
     */
    @Test
    public void testDbNameWithEscapedBacktickMatches() {
        givenRules(buildRule("ruleDbEscaped", "my`db", "*"));

        PrivilegeVerifyItem item = buildPrivilegeItem("`my``db`", TABLE_NAME);
        ExecutionContext ec = buildExecutionContext(DB_NAME, Lists.newArrayList(item));

        assertMatched(ec, "ruleDbEscaped");
    }

    /**
     * Scenario: a wildcard dbName ({@code *}) means the rule does not match on db, so the
     * presence of backticks in the schema must not affect matching.
     */
    @Test
    public void testWildcardDbNameNotAffectedByBacktick() {
        givenRules(buildRule("ruleWildcard", "*", "*"));

        ExecutionContext ec = buildExecutionContext("`anyschema`", Lists.newArrayList());

        assertMatched(ec, "ruleWildcard");
    }

    /**
     * Regression: table name carrying backticks must still match the configured rule table.
     */
    @Test
    public void testTableNameWithBacktickMatches() {
        givenRules(buildRule("ruleTableBacktick", "*", TABLE_NAME));

        PrivilegeVerifyItem item = buildPrivilegeItem(DB_NAME, "`" + TABLE_NAME + "`");
        ExecutionContext ec = buildExecutionContext(DB_NAME, Lists.newArrayList(item));

        assertMatched(ec, "ruleTableBacktick");
    }

    /**
     * Negative: stripping backticks must not over-match. A quoted schema that does not match the
     * rule dbName (after stripping) should not be matched.
     */
    @Test
    public void testBacktickSchemaDoesNotOverMatch() {
        givenRules(buildRule("ruleNoMatch", DB_NAME, "*"));

        ExecutionContext ec = buildExecutionContext("`other`", Lists.newArrayList());

        Assert.assertFalse(target.begin(ec));
        Assert.assertNull(ec.getCclContext());
    }

    private void givenRules(CclRuleInfo... rules) {
        when(mockCclConfigService.getCclRuleInfos()).thenReturn(Lists.newArrayList(rules));
    }

    private void assertMatched(ExecutionContext ec, String expectedRuleId) {
        boolean matched = target.begin(ec);
        Assert.assertTrue("CCL rule should be matched", matched);
        Assert.assertNotNull(ec.getCclContext());
        Assert.assertNotNull(ec.getCclContext().getCclRule());
        Assert.assertEquals(expectedRuleId, ec.getCclContext().getCclRule().getCclRuleRecord().id);
    }

    private CclRuleInfo buildRule(String id, String dbName, String tableName) {
        CclRuleRecord record = new CclRuleRecord();
        record.id = id;
        record.sqlType = "SELECT";
        record.dbName = dbName;
        record.tableName = tableName;
        record.userName = USER;
        record.clientIp = "%";
        record.parallelism = 10;
        record.queueSize = 10;
        record.keywords = null;
        return CclRuleInfo.create(record);
    }

    private PrivilegeVerifyItem buildPrivilegeItem(String db, String table) {
        PrivilegeVerifyItem item = new PrivilegeVerifyItem();
        item.setDb(db);
        item.setTable(table);
        item.setPrivilegePoint(PrivilegePoint.SELECT);
        return item;
    }

    private ExecutionContext buildExecutionContext(String schemaName, List<PrivilegeVerifyItem> privilegeVerifyItems) {
        ExecutionContext ec = new ExecutionContext();
        ec.setPrivilegeMode(true);
        ec.setConnId(1L);

        String originSql = "SELECT a FROM t1 WHERE id = 1";
        MockExecutionPlan plan = new MockExecutionPlan();
        plan.setCacheKey(newCacheKey(originSql));
        plan.setPrivilegeVerifyItems(privilegeVerifyItems);
        ec.setFinalPlan(plan);
        ec.setOriginSql(originSql);
        ec.setSchemaName(schemaName);

        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setUser(USER);
        privilegeContext.setHost(HOST);
        privilegeContext.setSchema(schemaName);
        ec.setPrivilegeContext(privilegeContext);

        return ec;
    }

    private static PlanCache.CacheKey newCacheKey(String originSql) {
        return new PlanCache.CacheKey("", originSql, "", Lists.newArrayList(), true, true, true);
    }

    private static class MockExecutionPlan extends ExecutionPlan {

        private PlanCache.CacheKey cacheKey;

        MockExecutionPlan() {
            super(null, null, null);
        }

        void setCacheKey(PlanCache.CacheKey cacheKey) {
            this.cacheKey = cacheKey;
        }

        @Override
        public PlanCache.CacheKey getCacheKey() {
            return this.cacheKey;
        }
    }

}

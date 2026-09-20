package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

public class RoutingRuleClassifierTest {

    private static final String allUser = RoutingRuleManager.ALL_USER;
    private static final String user1 = "gg1";
    private static final String user2 = "gg2";
    private static final String templateId = "temp";
    private static final String sql = "select 1";
    private int id = 1;

    @Test
    public void testEmpty() {
        RoutingRuleClassifier classifier;
        ExecutionContext ec = new ExecutionContext("hello");
        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setUser(user1);
        ec.setPrivilegeContext(privilegeContext);
        classifier = RoutingRuleClassifier.build(null);
        Assert.assertNull(classifier.classify(ec, templateId, sql));
        classifier = RoutingRuleClassifier.build(ImmutableList.of());
        Assert.assertNull(classifier.classify(ec, templateId, sql));

        classifier = RoutingRuleClassifier.build(ImmutableList.of(
            userRoutingRule(user2, RoutingType.COLUMNAR),
            userRoutingRule(user1, RoutingType.ROW)));
        Assert.assertNull(classifier.classify(ec, templateId, sql));

        classifier = RoutingRuleClassifier.build(Arrays.asList(
            keywordsRoutingRule(user1, ",", RoutingType.COLUMNAR)));
        Assert.assertNull(classifier.classify(ec, null, null));

        privilegeContext.setUser(null);
        Assert.assertNull(classifier.classify(ec, null, null));
    }

    @Test
    public void testUserRouting() {
        ExecutionContext ec = new ExecutionContext("hello");
        PrivilegeContext privilegeContext = new PrivilegeContext();
        ec.setPrivilegeContext(privilegeContext);
        this.id = 1;
        RoutingRuleClassifier classifier = RoutingRuleClassifier.build(Arrays.asList(
            userRoutingRule(allUser, RoutingType.HTAP),
            userRoutingRule(user2, RoutingType.HTAP),
            userRoutingRule(user2, RoutingType.COLUMNAR),
            userRoutingRule(user2, RoutingType.COLUMNAR),
            userRoutingRule(user1, RoutingType.ROW),
            userRoutingRule(user1, RoutingType.COLUMNAR)
        ));

        privilegeContext.setUser(user2);
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 3L));

        privilegeContext.setUser(user1);
        Assert.assertEquals(RoutingType.ROW, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 5L));

        privilegeContext.setUser(user1 + "1");
        Assert.assertNull(classifier.classify(ec, templateId, sql));

        Assert.assertEquals(0L, getIdCounter(classifier, 1L));
        Assert.assertEquals(0L, getIdCounter(classifier, 2L));
        Assert.assertEquals(1L, getIdCounter(classifier, 3L));
        Assert.assertEquals(0L, getIdCounter(classifier, 4L));
        Assert.assertEquals(1L, getIdCounter(classifier, 5L));
        Assert.assertEquals(0L, getIdCounter(classifier, 6L));
    }

    @Test
    public void testKeywordRouting() throws Exception {
        ExecutionContext ec = new ExecutionContext("hello");
        PrivilegeContext privilegeContext = new PrivilegeContext();
        ec.setPrivilegeContext(privilegeContext);
        this.id = 1;
        RoutingRuleClassifier classifier = RoutingRuleClassifier.build(Arrays.asList(
            keywordsRoutingRule(allUser, "select", RoutingType.HTAP),
            keywordsRoutingRule(user2, "", RoutingType.HTAP),
            keywordsRoutingRule(allUser, "select", RoutingType.COLUMNAR),
            keywordsRoutingRule(user1, "select", RoutingType.ROW),
            keywordsRoutingRule(user1, "select", RoutingType.COLUMNAR)
        ));
        Class<?> clazz = RoutingRuleClassifier.class;
        Field privateField = clazz.getDeclaredField("keywordRoutingCache");
        privateField.setAccessible(true);
        Cache<Object, Object> value = (Cache<Object, Object>) privateField.get(classifier);

        privilegeContext.setUser(user2);
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1, value.size());
        Assert.assertEquals(1L, getIdCounter(classifier, 3L));

        privilegeContext.setUser(user1);
        Assert.assertEquals(RoutingType.ROW, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(2, value.size());
        Assert.assertEquals(1L, getIdCounter(classifier, 4L));

        privilegeContext.setUser(user1 + "1");
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(3, value.size());
        Assert.assertEquals(2L, getIdCounter(classifier, 3L));

        privilegeContext.setUser(user1);
        Assert.assertEquals(RoutingType.ROW, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(3, value.size());
        Assert.assertEquals(2L, getIdCounter(classifier, 4L));

        privilegeContext.setUser(user1 + "2");
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, "", sql));
        Assert.assertEquals(3, value.size());
        Assert.assertEquals(3L, getIdCounter(classifier, 3L));

        Assert.assertEquals(0L, getIdCounter(classifier, 1L));
        Assert.assertEquals(0L, getIdCounter(classifier, 2L));
        Assert.assertEquals(3L, getIdCounter(classifier, 3L));
        Assert.assertEquals(2L, getIdCounter(classifier, 4L));
        Assert.assertEquals(0L, getIdCounter(classifier, 5L));
    }

    @Test
    public void testTemplateRouting() {
        ExecutionContext ec = new ExecutionContext("hello");
        PrivilegeContext privilegeContext = new PrivilegeContext();
        ec.setPrivilegeContext(privilegeContext);
        this.id = 1;
        RoutingRuleClassifier classifier = RoutingRuleClassifier.build(Arrays.asList(
            templateRoutingRule(allUser, templateId, RoutingType.HTAP),
            templateRoutingRule(user2, templateId, RoutingType.HTAP),
            templateRoutingRule(allUser, templateId, RoutingType.COLUMNAR),
            templateRoutingRule(user1, templateId, RoutingType.ROW),
            templateRoutingRule(user1, templateId, RoutingType.COLUMNAR)
        ));

        privilegeContext.setUser(user2);
        Assert.assertEquals(RoutingType.HTAP, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 2L));

        privilegeContext.setUser(user1);
        Assert.assertEquals(RoutingType.ROW, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 4L));

        privilegeContext.setUser(user1 + "1");
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 3L));

        privilegeContext.setUser(user1);
        Assert.assertNull(classifier.classify(ec, null, sql));

        Assert.assertEquals(0L, getIdCounter(classifier, 1L));
        Assert.assertEquals(1L, getIdCounter(classifier, 2L));
        Assert.assertEquals(1L, getIdCounter(classifier, 3L));
        Assert.assertEquals(1L, getIdCounter(classifier, 4L));
        Assert.assertEquals(0L, getIdCounter(classifier, 5L));
    }

    @Test
    public void testCompositeRouting() {
        String user3 = user1 + "1";
        String missSql = "show tables";
        String missTemplateId = templateId + "1";
        ExecutionContext ec = new ExecutionContext("hello");
        PrivilegeContext privilegeContext = new PrivilegeContext();
        ec.setPrivilegeContext(privilegeContext);
        this.id = 1;
        RoutingRuleClassifier classifier = RoutingRuleClassifier.build(Arrays.asList(
            keywordsRoutingRule(allUser, "select", RoutingType.COLUMNAR),
            templateRoutingRule(allUser, templateId, RoutingType.ROW),
            templateRoutingRule(user3, templateId, RoutingType.COLUMNAR),
            keywordsRoutingRule(user2, "tables", RoutingType.HTAP),
            templateRoutingRule(user1, templateId, RoutingType.HTAP),
            keywordsRoutingRule(user1, "select", RoutingType.ROW),
            userRoutingRule(user1, RoutingType.COLUMNAR)
        ));
        Assert.assertFalse(classifier.isHasFollowerRead());
        // user1 has template, keyword, user
        privilegeContext.setUser(user1);
        // template
        Assert.assertEquals(RoutingType.HTAP, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 5L));
        // keyword
        Assert.assertEquals(RoutingType.ROW, classifier.classify(ec, null, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 6L));
        // user
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, null, missSql));
        Assert.assertEquals(1L, getIdCounter(classifier, 7L));
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, missTemplateId, missSql));
        Assert.assertEquals(2L, getIdCounter(classifier, 7L));

        // user2 has keyword
        privilegeContext.setUser(user2);
        // template
        Assert.assertEquals(RoutingType.ROW, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 2L));
        // keyword
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, null, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 1L));
        Assert.assertEquals(RoutingType.HTAP, classifier.classify(ec, missTemplateId, missSql));
        Assert.assertEquals(1L, getIdCounter(classifier, 4L));
        Assert.assertEquals(RoutingType.HTAP, classifier.classify(ec, null, missSql));
        Assert.assertEquals(2L, getIdCounter(classifier, 4L));
        Assert.assertNull(classifier.classify(ec, null, "show databases"));

        // user3 has template
        privilegeContext.setUser(user3);
        // template
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(1L, getIdCounter(classifier, 3L));
        // keyword
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, null, sql));
        Assert.assertEquals(2L, getIdCounter(classifier, 1L));
        Assert.assertNull(classifier.classify(ec, null, "show databases"));

        Assert.assertEquals(2L, getIdCounter(classifier, 1L));
        Assert.assertEquals(1L, getIdCounter(classifier, 2L));
        Assert.assertEquals(1L, getIdCounter(classifier, 3L));
        Assert.assertEquals(2L, getIdCounter(classifier, 4L));
        Assert.assertEquals(1L, getIdCounter(classifier, 5L));
        Assert.assertEquals(1L, getIdCounter(classifier, 6L));
        Assert.assertEquals(2L, getIdCounter(classifier, 7L));
    }

    @Test
    public void testFollowerRouting() {
        ExecutionContext ec = new ExecutionContext("hello");
        PrivilegeContext privilegeContext = new PrivilegeContext();
        ec.setPrivilegeContext(privilegeContext);
        this.id = 1;
        RoutingRuleClassifier classifier = RoutingRuleClassifier.build(Arrays.asList(
            keywordsRoutingRule(allUser, "select", RoutingType.COLUMNAR),
            templateRoutingRule(allUser, templateId, RoutingType.ROW),
            templateRoutingRule(user1, templateId, RoutingType.HTAP),
            userRoutingRule(user2, RoutingType.FOLLOWER),
            keywordsRoutingRule(user2, "select", RoutingType.HTAP),
            userRoutingRule(user2, RoutingType.COLUMNAR),
            userRoutingRule(user2, RoutingType.COLUMNAR)
        ));
        Assert.assertTrue(classifier.isHasFollowerRead());

        privilegeContext.setUser(user1);
        Assert.assertEquals(RoutingType.COLUMNAR, classifier.classify(ec, null, sql));
        Assert.assertEquals(RoutingType.HTAP, classifier.classify(ec, templateId, sql));
        Assert.assertNull(classifier.classify(ec, null, "show databases"));

        privilegeContext.setUser(user2);
        Assert.assertEquals(RoutingType.FOLLOWER, classifier.classify(ec, null, sql));
        Assert.assertEquals(RoutingType.FOLLOWER, classifier.classify(ec, templateId, sql));
        Assert.assertEquals(RoutingType.FOLLOWER, classifier.classify(ec, null, "show databases"));

        Assert.assertEquals(1L, getIdCounter(classifier, 1L));
        Assert.assertEquals(0L, getIdCounter(classifier, 2L));
        Assert.assertEquals(1L, getIdCounter(classifier, 3L));
        Assert.assertEquals(3L, getIdCounter(classifier, 4L));
        Assert.assertEquals(0L, getIdCounter(classifier, 5L));
        Assert.assertEquals(0L, getIdCounter(classifier, 6L));
        Assert.assertEquals(0L, getIdCounter(classifier, 7L));
    }

    RoutingRuleRecord userRoutingRule(String user, RoutingType routingType) {
        RoutingRuleRecord record = new RoutingRuleRecord();
        record.id = id++;
        record.userName = user;
        record.routingType = routingType.name();
        return record;
    }

    RoutingRuleRecord keywordsRoutingRule(String user, String keywords, RoutingType routingType) {
        RoutingRuleRecord record = new RoutingRuleRecord();
        record.id = id++;
        record.userName = user;
        record.keywords = Arrays.asList(keywords);
        record.routingType = routingType.name();
        return record;
    }

    RoutingRuleRecord templateRoutingRule(String user, String template, RoutingType routingType) {
        RoutingRuleRecord record = new RoutingRuleRecord();
        record.id = id++;
        record.userName = user;
        record.templateId = template;
        record.routingType = routingType.name();
        return record;
    }

    long getIdCounter(RoutingRuleClassifier classifier, long id) {
        return classifier.getIdCounterMap().get(id);
    }
}

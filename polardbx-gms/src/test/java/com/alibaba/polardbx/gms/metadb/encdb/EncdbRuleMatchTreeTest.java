package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.*;
import static com.alibaba.polardbx.gms.metadb.encdb.EncdbRule.jsonArr2Set;

public class EncdbRuleMatchTreeTest extends TestCase {

    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testEncAll() {
        String ruleName = "encdb_all_rule";
        EncdbRule encdbRule = new EncdbRule(ruleName, true,
            Collections.EMPTY_SET,
            Collections.EMPTY_SET,
            Collections.singleton("*"),
            Collections.singleton("*"),
            Collections.singleton("*"),
            ruleName,
            EncdbRule.EncdbRuleType.ENCRYPTION,
            null);

        EncdbRuleMatchTree encdbRuleMatchTree = new EncdbRuleMatchTree();
        encdbRuleMatchTree.insertRule(encdbRule);

        for (int i = 0; i < 100; i++) {
            //匹配所有字段
            Assert.assertEquals(ruleName, encdbRuleMatchTree.match(randString(), randString(), randString()));
            //匹配所有用户
            Assert.assertNull(encdbRule.isRestrictedAccess(PolarAccount.fromIdentifier("username_" + randString())));
            Assert.assertTrue(ruleName, encdbRuleMatchTree.fastMatch(randString(), randString()));
        }

    }

    private static String randString() {
        return ThreadLocalRandom.current().nextInt(1000000, 9999999) + "_rand_str";
    }

}
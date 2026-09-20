package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.node.CCLDetectManager.DEFAULT_ROOT_COLUMN;
import static com.alibaba.polardbx.gms.node.CCLDetectManager.UNDETERMINED_COLUMN;
import static com.alibaba.polardbx.gms.node.CCLDetectUtils.DubiousItem;

/**
 * Unit test for CCLDetectUtils
 *
 * @author liugaoji
 */
@RunWith(MockitoJUnitRunner.class)
public class CCLDetectUtilsTest {

    @Mock
    private DynamicConfig mockDynamicConfig;

    @Mock
    private CCLDetectConfig mockCclDetectConfig;

    @Test
    public void testDubiousItemToString() {
        DubiousItem item = new DubiousItem(1L, "SELECT * FROM test", 1000L);
        String result = item.toString();
        Assert.assertEquals("Should format correctly", "1;SELECT * FROM test 1000", result);
    }

    @Test
    public void testDubiousItemToStringWithNullValues() {
        DubiousItem item = new DubiousItem(null, null, null);
        String result = item.toString();
        Assert.assertEquals("Should handle null values", "null;null null", result);
    }

    @Test
    public void testConfigIsCclDetectEnable() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.isCclDetectEnable()).thenReturn(true);

            CCLDetectConfig config = new CCLDetectConfig();
            boolean result = config.isCclDetectEnable();

            Assert.assertTrue("Should return true when CCL detect is enabled", result);
        }
    }

    @Test
    public void testConfigGetRootColumn() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectRootColumn()).thenReturn("test_column");

            CCLDetectConfig config = new CCLDetectConfig();
            String result = config.getRootColumn();

            Assert.assertEquals("Should return correct root column", "test_column", result);
        }
    }

    @Test
    public void testConfigGetConnectionLimit() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectConnectionLimit()).thenReturn(100);

            CCLDetectConfig config = new CCLDetectConfig();
            int result = config.getConnectionLimit();

            Assert.assertEquals("Should return correct connection limit", 100, result);
        }
    }

    @Test
    public void testConfigGetKillBatch() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectKillBatch()).thenReturn(50);

            CCLDetectConfig config = new CCLDetectConfig();
            int result = config.getKillBatch();

            Assert.assertEquals("Should return correct kill batch", 50, result);
        }
    }

    @Test
    public void testConfigGetKillMinConcurrency() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectKillMinConcurrency()).thenReturn(5);

            CCLDetectConfig config = new CCLDetectConfig();
            int result = config.getKillMinConcurrency();

            Assert.assertEquals("Should return correct kill min concurrency", 5, result);
        }
    }

    @Test
    public void testConfigGetSlowThreshold() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectSlowThreshold()).thenReturn(1000);

            CCLDetectConfig config = new CCLDetectConfig();
            int result = config.getSlowThreshold();

            Assert.assertEquals("Should return correct slow threshold", 1000, result);
        }
    }

    @Test
    public void testConfigGetMaxThreshold() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectMaxThreshold()).thenReturn(10000);

            CCLDetectConfig config = new CCLDetectConfig();
            int result = config.getMaxThreshold();

            Assert.assertEquals("Should return correct max threshold", 10000, result);
        }
    }

    @Test
    public void testConfigGetDnDelayInterval() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectDnDelayInterval()).thenReturn(30);

            CCLDetectConfig config = new CCLDetectConfig();
            long result = config.getDnDelayInterval();

            Assert.assertEquals("Should return correct DN delay interval", 30L, result);
        }
    }

    @Test
    public void testConfigGetDnRuleExpireTime() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectDnRuleExpireTime()).thenReturn(300);

            CCLDetectConfig config = new CCLDetectConfig();
            int result = config.getDnRuleExpireTime();

            Assert.assertEquals("Should return correct DN rule expire time", 300, result);
        }
    }

    @Test
    public void testParseTemplateIdAndRootColumnSuccess() {
        String sql = "/*DRDS /127.0.0.1/195881395b000000/0///////template123/ */SELECT * FROM test";
        String result = CCLDetectUtils.parseTemplateIdAndRootColumn(sql);
        Assert.assertEquals("Should extract template ID correctly", "template123", result);
    }

    @Test
    public void testParseTemplateIdAndRootColumnWithInsufficientParts() {
        String sql = "/*DRDS /127.0.0.1/195881395b000000/ */SELECT * FROM test";
        String result = CCLDetectUtils.parseTemplateIdAndRootColumn(sql);
        Assert.assertNull("Should return null when insufficient parts", result);
    }

    @Test
    public void testParseTemplateIdAndRootColumnWithNullInput() {
        String result = CCLDetectUtils.parseTemplateIdAndRootColumn(null);
        Assert.assertNull("Should return null for null input", result);
    }

    @Test
    public void testParseTemplateIdAndRootColumnWithInvalidFormat() {
        String sql = "SELECT * FROM test";
        String result = CCLDetectUtils.parseTemplateIdAndRootColumn(sql);
        Assert.assertNull("Should return null for invalid format", result);
    }

    @Test
    public void testParseTemplateIdAndRootColumnWithMissingPrefix() {
        String sql = "127.0.0.1/195881395b000000/0//template123/ */SELECT * FROM test";
        String result = CCLDetectUtils.parseTemplateIdAndRootColumn(sql);
        Assert.assertNull("Should return null when missing prefix", result);
    }

    @Test
    public void testParseTemplateIdAndRootColumnWithMissingSuffix() {
        String sql = "/*DRDS /127.0.0.1/195881395b000000/0//template123/ SELECT * FROM test";
        String result = CCLDetectUtils.parseTemplateIdAndRootColumn(sql);
        Assert.assertNull("Should return null when missing suffix", result);
    }

    @Test
    public void testIsOutTrxSqlTrue() {
        String sql = "/*DRDS /127.0.0.1/195881395b000000/0//template123/ */SELECT * FROM test";
        boolean result = CCLDetectUtils.isOutTrxSql(sql);
        Assert.assertTrue("Should return true for out-transaction SQL", result);
    }

    @Test
    public void testIsOutTrxSqlFalse() {
        String sql = "/*DRDS /127.0.0.1/195881395b000000-456/0//template123/ */SELECT * FROM test";
        boolean result = CCLDetectUtils.isOutTrxSql(sql);
        Assert.assertFalse("Should return false for in-transaction SQL", result);
    }

    @Test
    public void testIsOutTrxSqlWithNullInput() {
        boolean result = CCLDetectUtils.isOutTrxSql(null);
        Assert.assertFalse("Should return false for null input", result);
    }

    @Test
    public void testIsOutTrxSqlWithInvalidFormat() {
        String sql = "SELECT * FROM test";
        boolean result = CCLDetectUtils.isOutTrxSql(sql);
        Assert.assertFalse("Should return false for invalid format", result);
    }

    @Test
    public void testIsRootColumnWithExactMatch() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectRootColumn()).thenReturn("test_column");

            boolean result = CCLDetectUtils.isRootColumn("test_column", "part_key");

            Assert.assertTrue("Should return true for exact match", result);
        }
    }

    @Test
    public void testIsRootColumnWithDefaultRootColumnMatch() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectRootColumn()).thenReturn(DEFAULT_ROOT_COLUMN);

            boolean result = CCLDetectUtils.isRootColumn("part_key", "part_key");

            Assert.assertTrue("Should return true for default root column match", result);
        }
    }

    @Test
    public void testIsRootColumnWithNoMatch() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectRootColumn()).thenReturn("other_column");

            boolean result = CCLDetectUtils.isRootColumn("test_column", "part_key");

            Assert.assertFalse("Should return false when no match", result);
        }
    }

    @Test
    public void testIsRootColumnWithException() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectRootColumn()).thenThrow(new RuntimeException("Test exception"));

            boolean result = CCLDetectUtils.isRootColumn("test_column", "part_key");

            Assert.assertFalse("Should return false when exception occurs", result);
        }
    }

    @Test
    public void testHasOutTrxWithSlowOutTrxSql() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectSlowThreshold()).thenReturn(1000);

            // Create test data
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(
                new DubiousItem(1L, "/*DRDS /127.0.0.1/195881395b000000/0///////template1/ */SELECT * FROM test",
                    1500000L));
            infos.add(
                new DubiousItem(2L, "/*DRDS /127.0.0.1/195881395b000000/0///////template1/ */SELECT * FROM test",
                    2000000L));

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();

            boolean result = CCLDetectUtils.hasOutTrx(infos, interceptTime, interceptInfos, false);

            Assert.assertTrue("Should return true when has slow out-transaction SQL", result);
            Assert.assertEquals("Should aggregate time correctly", Long.valueOf(3500000L),
                interceptTime.get("template1").getKey());
            Assert.assertEquals("Should count correctly", Integer.valueOf(2),
                interceptTime.get("template1").getValue());
            Assert.assertEquals("Should group items correctly", 2, interceptInfos.get("template1").size());
        }
    }

    @Test
    public void testHasOutTrxWithFastOutTrxSql() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

            // Create test data with fast queries
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(
                new DubiousItem(1L, "/*DRDS /127.0.0.1/195881395b000000/0//template2/ */SELECT * FROM test", 1000L));
            infos.add(
                new DubiousItem(2L, "/*DRDS /127.0.0.1/195881395b000000/0//template2/ */SELECT * FROM test", 2000L));

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();

            boolean result = CCLDetectUtils.hasOutTrx(infos, interceptTime, interceptInfos, false);

            Assert.assertFalse("Should return false when no slow out-transaction SQL", result);
        }
    }

    @Test
    public void testHasOutTrxWithInTrxSql() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

            // Create test data with in-transaction SQL
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(new DubiousItem(1L, "/*DRDS /127.0.0.1/195881395b000000-456/0//template3/ */SELECT * FROM test",
                5000L));

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();

            boolean result = CCLDetectUtils.hasOutTrx(infos, interceptTime, interceptInfos, false);

            Assert.assertFalse("Should return false for in-transaction SQL", result);
        }
    }

    @Test
    public void testHasOutTrxWithMixedSql() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectSlowThreshold()).thenReturn(1000);

            // Create test data with mixed SQL types
            List<DubiousItem> infos = new ArrayList<>();
            // Out-transaction slow SQL
            infos.add(
                new DubiousItem(1L, "/*DRDS /127.0.0.1/195881395b000000/0///////template4/ */SELECT * FROM test",
                    1500000L));
            // In-transaction slow SQL
            infos.add(
                new DubiousItem(2L, "/*DRDS /127.0.0.1/195881395b000000-456/0///////template5/ */SELECT * FROM test",
                    4000L));
            // Out-transaction fast SQL
            infos.add(
                new DubiousItem(3L, "/*DRDS /127.0.0.1/195881395b000000/0///////template6/ */SELECT * FROM test",
                    500L));

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();

            boolean result = CCLDetectUtils.hasOutTrx(infos, interceptTime, interceptInfos, false);

            Assert.assertTrue("Should return true when has slow out-transaction SQL", result);
            Assert.assertEquals("Should have 3 different templates", 3, interceptTime.size());
        }
    }

    @Test
    public void testHasOutTrxWithInvalidHints() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

            // Create test data with invalid hints
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(new DubiousItem(1L, "SELECT * FROM test", 5000L)); // No hint
            infos.add(new DubiousItem(2L, "/*INVALID HINT*/SELECT * FROM test", 6000L)); // Invalid hint

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();

            boolean result = CCLDetectUtils.hasOutTrx(infos, interceptTime, interceptInfos, false);

            Assert.assertFalse("Should return false when no valid hints", result);
            Assert.assertTrue("Should not process invalid hints", interceptTime.isEmpty());
            Assert.assertTrue("Should not process invalid hints", interceptInfos.isEmpty());
        }
    }

    @Test
    public void testHasOutTrxWithEmptyList() {
        List<DubiousItem> infos = new ArrayList<>();
        Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
        Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();

        boolean result = CCLDetectUtils.hasOutTrx(infos, interceptTime, interceptInfos, false);

        Assert.assertFalse("Should return false for empty list", result);
        Assert.assertTrue("Should not populate interceptTime", interceptTime.isEmpty());
        Assert.assertTrue("Should not populate interceptInfos", interceptInfos.isEmpty());
    }

    @Test
    public void testHasOutTrxWithNullKey() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

            // Create test data with SQL that returns null key
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(
                new DubiousItem(1L, "/*DRDS /127.0.0.1/195881395b000000/0// */SELECT * FROM test", 5000L)); // Empty key

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();

            boolean result = CCLDetectUtils.hasOutTrx(infos, interceptTime, interceptInfos, false);

            Assert.assertFalse("Should return false when key is null or empty", result);
            Assert.assertTrue("Should not process items with null/empty keys", interceptTime.isEmpty());
        }
    }
}
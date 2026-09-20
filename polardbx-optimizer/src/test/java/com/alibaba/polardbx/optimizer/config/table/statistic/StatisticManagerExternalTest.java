package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StatisticManagerExternalTest {

    private InstanceRole originalRole;

    @Before
    public void setup() {
        // Fast mock makes doInit() a no-op, so the singleton can be created without a MetaDB.
        originalRole = ConfigDataMode.getInstanceRole();
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        StatisticManager stats = StatisticManager.getInstance();
        stats.setRowCount("hive$$dwd", "orders", 4242);
        stats.setRowCount("hive$$ods", "events", 17);
        stats.setRowCount("hive_ext$$db", "keepme", 5);
        stats.setRowCount("local_db", "users", 9);
    }

    @After
    public void teardown() {
        StatisticManager.getInstance().getStatisticCache().clear();
        ConfigDataMode.setInstanceRole(originalRole);
    }

    @Test
    public void removeExternalSchemaStatisticsDropsEveryDbOfTheCatalog() {
        StatisticManager.removeExternalSchemaStatistics("hive");

        Assert.assertFalse(StatisticManager.getInstance().getStatisticCache().containsKey("hive$$dwd"));
        Assert.assertFalse(StatisticManager.getInstance().getStatisticCache().containsKey("hive$$ods"));
    }

    @Test
    public void removeExternalSchemaStatisticsMatchesTheExactCatalogPrefix() {
        StatisticManager.removeExternalSchemaStatistics("hive");

        Assert.assertTrue("a catalog whose name merely starts with 'hive' must survive",
            StatisticManager.getInstance().getStatisticCache().containsKey("hive_ext$$db"));
        Assert.assertTrue("local schemas must survive",
            StatisticManager.getInstance().getStatisticCache().containsKey("local_db"));
    }
}

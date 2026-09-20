package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.FiredTtlJobItem;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.FiredTtlJobSorter;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenhui.lch
 */
public class FiredTtlJobSorterTest {

    @Test
    public void test_sortQueuingFiredTtlJobItemList() throws SQLException {
        try {

            List<FiredTtlJobItem> runningTtlJobItemList = new ArrayList<>();
            List<FiredTtlJobItem> queuingTtlJobItemList = new ArrayList<>();

            String[][] runningTtlJobItemTgKeyArr = new String[][] {
                // fireTime,dbIdx, tgId, tbIdx
                {"1", "1", "1", "1"},
                {"1", "1", "1", "2"},
                {"1", "2", "1", "3"},
            };
            runningTtlJobItemList = buildFiredJobItemList(runningTtlJobItemTgKeyArr);

            String[][] queuingTtlJobItemTgKeyArr = new String[][] {
                // fireTime,dbIdx, tgId, tbIdx
                {"1", "1", "1", "4"},
                {"1", "1", "1", "5"},
                {"1", "1", "1", "6"},
                {"1", "2", "1", "7"},
                {"1", "2", "2", "8"},
                {"1", "3", "3", "9"},

            };
            queuingTtlJobItemList = buildFiredJobItemList(queuingTtlJobItemTgKeyArr);

            List<FiredTtlJobItem> newSortedQueuingTtlJobItemList =
                FiredTtlJobSorter.sortQueuingFiredTtlJobItemList(runningTtlJobItemList, queuingTtlJobItemList);

//            tgKey: 1#db2#2, tbName: tb8
//            tgKey: 1#db3#3, tbName: tb9
//            tgKey: 1#db1#1, tbName: tb4
//            tgKey: 1#db1#1, tbName: tb5
//            tgKey: 1#db1#1, tbName: tb6
//            tgKey: 1#db2#1, tbName: tb7
            String[][] sortResultInfo = new String[][] {
                {"1#db2#2", "tb8"},
                {"1#db3#3", "tb9"},
                {"1#db1#1", "tb4"},
                {"1#db1#1", "tb5"},
                {"1#db1#1", "tb6"},
                {"1#db2#1", "tb7"},
            };
            for (int i = 0; i < newSortedQueuingTtlJobItemList.size(); i++) {
                FiredTtlJobItem item = newSortedQueuingTtlJobItemList.get(i);
                String tgKey = item.getJobItemTgKey();
                String tbName = item.getTableMeta().getTableName();
                String targetTgKey = sortResultInfo[i][0];
                String targetTbName = sortResultInfo[i][1];
                Assert.assertTrue(tgKey.equalsIgnoreCase(targetTgKey));
                Assert.assertTrue(tbName.equalsIgnoreCase(targetTbName));
                System.out.println("tgKey: " + tgKey + ", tbName: " + tbName);

            }
        } catch (Throwable ex) {
            ex.getMessage();
            Assert.fail(ex.getMessage());
        }

    }

    protected static List<FiredTtlJobItem> buildFiredJobItemList(String[][] queuingTtlJobItemTgKeyArr) {
        List<FiredTtlJobItem> queuingTtlJobItemList = new ArrayList<>();
        for (int i = 0; i < queuingTtlJobItemTgKeyArr.length; i++) {
            String[] queuingTtlJobItemInfo = queuingTtlJobItemTgKeyArr[i];
            Long fireTime = Long.valueOf(queuingTtlJobItemInfo[0]);
            Long dbIdx = Long.valueOf(queuingTtlJobItemInfo[1]);
            Long tgId = Long.valueOf(queuingTtlJobItemInfo[2]);
            Long tbIdx = Long.valueOf(queuingTtlJobItemInfo[3]);
            String tableSchemaName = "db" + dbIdx;
            String tableName = "tb" + tbIdx;
            String jobItemTgKey =
                String.format(FiredTtlJobItem.FIRED_TTL_JOB_TG_KEY_TEMPLATE, fireTime, tableSchemaName, tgId);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(partitionInfo.getTableGroupId()).thenReturn(tgId);
            Mockito.when(partitionInfo.getTableSchema()).thenReturn(tableSchemaName);
            Mockito.when(partitionInfo.getTableName()).thenReturn(tableName);
            TableMeta mockTableMeta = Mockito.mock(TableMeta.class);
            Mockito.when(mockTableMeta.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(mockTableMeta.getTableName()).thenReturn(tableName);
            Mockito.when(mockTableMeta.getSchemaName()).thenReturn(tableSchemaName);

            ExecutableScheduledJob job = Mockito.mock(ExecutableScheduledJob.class);
            Mockito.when(job.getFireTime()).thenReturn(fireTime);

            FiredTtlJobItem mockFiredTtlJobItem = Mockito.mock(FiredTtlJobItem.class);
            Mockito.when(mockFiredTtlJobItem.getTableMeta()).thenReturn(mockTableMeta);
            Mockito.when(mockFiredTtlJobItem.getJob()).thenReturn(job);
            Mockito.when(mockFiredTtlJobItem.getJobItemTgKey()).thenReturn(jobItemTgKey);

            queuingTtlJobItemList.add(mockFiredTtlJobItem);
        }
        return queuingTtlJobItemList;
    }
}

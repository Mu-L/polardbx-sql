package com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FiredTtlJobSorter {

    protected static class FiredTtlJobItemComparator implements Comparator<FiredTtlJobItem> {

        protected List<FiredTtlJobItem> firedRunningTtlJobItemList = new ArrayList<>();
        protected Map<String, List<FiredTtlJobItem>> runningTtlJobItemTgKeyMapping =
            new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        public FiredTtlJobItemComparator(List<FiredTtlJobItem> firedRunningTtlJobItems) {
            this.firedRunningTtlJobItemList = firedRunningTtlJobItems;
            for (int i = 0; i < firedRunningTtlJobItems.size(); i++) {
                FiredTtlJobItem jobItem = firedRunningTtlJobItems.get(i);
                String jobItemTgKey = jobItem.getJobItemTgKey();
                List<FiredTtlJobItem> jobItemList = runningTtlJobItemTgKeyMapping.get(jobItemTgKey);
                if (jobItemList == null) {
                    jobItemList = new ArrayList<>();
                    runningTtlJobItemTgKeyMapping.put(jobItemTgKey, jobItemList);
                }
                jobItemList.add(jobItem);
            }
        }

        @Override
        public int compare(FiredTtlJobItem jobItem1, FiredTtlJobItem jobItem2) {

            // 1, compare fireTime
            ExecutableScheduledJob job1 = jobItem1.getJob();
            ExecutableScheduledJob job2 = jobItem2.getJob();
            int timeCompare = Long.compare(job1.getFireTime(), job2.getFireTime());
            if (timeCompare != 0) {
                return timeCompare;
            }

            // 2, check if the jobItemTgKey is running
            String jobItemKey1 = jobItem1.getJobItemTgKey();
            String jobItemKey2 = jobItem2.getJobItemTgKey();
            boolean findRunningTgOnTgKey1 = runningTtlJobItemTgKeyMapping.containsKey(jobItemKey1);
            boolean findRunningTgOnTgKey2 = runningTtlJobItemTgKeyMapping.containsKey(jobItemKey2);

            if (!findRunningTgOnTgKey1 && findRunningTgOnTgKey2) {
                return -1;
            } else if (findRunningTgOnTgKey1 && !findRunningTgOnTgKey2) {
                return 1;
            }

            // 3, compare dbName
            TableMeta tbMeta1 = jobItem1.getTableMeta();
            TableMeta tbMeta2 = jobItem2.getTableMeta();
            String db1 = tbMeta1.getSchemaName();
            String db2 = tbMeta2.getSchemaName();
            int dbComp = db1.compareToIgnoreCase(db2);
            if (dbComp != 0) {
                return dbComp;
            }

            // 4, compare tgName
            Long tgId1 = tbMeta1.getPartitionInfo().getTableGroupId();
            Long tgId2 = tbMeta2.getPartitionInfo().getTableGroupId();
            int tgIdComp = tgId1.compareTo(tgId2);
            if (tgIdComp != 0) {
                return tgIdComp;
            }

            // 5, compare tblName
            String tb1 = tbMeta1.getTableName();
            String tb2 = tbMeta2.getTableName();
            int tbComp = tb1.compareToIgnoreCase(tb2);
            return tbComp;

        }
    }

    public static List<FiredTtlJobItem> sortQueuingFiredTtlJobItemList(
        List<FiredTtlJobItem> firedRunningTtlJobItemList,
        List<FiredTtlJobItem> firedQueuingTtlJobItemList) {
        List<FiredTtlJobItem> sortedFiredTtlJobItemList = new ArrayList<>();
        FiredTtlJobItemComparator comparator = new FiredTtlJobItemComparator(firedRunningTtlJobItemList);
        sortedFiredTtlJobItemList.addAll(firedQueuingTtlJobItemList);
        sortedFiredTtlJobItemList.sort(comparator);
        return sortedFiredTtlJobItemList;
    }
}

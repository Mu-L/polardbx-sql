/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Compute location for various partition operations:
 * 1. create partition-table
 * 2. split-partition
 * 3. partition-balance
 * ......
 *
 * @author moyi
 * @since 2021/04
 */
public class TableGroupLocation {

    /**
     * Choose a group to place new partition, which should has the least physical table
     *
     * @param schema the schema of the new partition
     */
    public static GroupDetailInfoExRecord chooseGroupForNewPartition(String schema) {
        return getOrderedGroupList(schema).get(0);
    }

    public static GroupAllocator buildGroupAllocator(String schema, LocalityDesc localityDesc) {
        if (localityDesc.holdEmptyDnList()) {
            return new GroupAllocator(getOrderedGroupList(schema));
        } else {
            return buildGroupAllocatorByLocality(schema, localityDesc);
        }
    }

    public static GroupAllocator buildGroupAllocatorOfPartitionByLocality(String schema, LocalityDesc localityDesc) {
        List<GroupDetailInfoExRecord> groups = getOrderedGroupList(schema);
        groups = groups.stream()
            .filter(x -> localityDesc.matchStorageInstance(x.getStorageInstId()))
            .collect(Collectors.toList());
        return new GroupAllocator(groups);

    }

    public static GroupAllocator buildGroupAllocatorByGroup(String schema, List<GroupDetailInfoExRecord> groups) {
        return new GroupAllocator(groups);
    }

    public static GroupAllocator buildGroupAllocatorByGroup(String schema, List<GroupDetailInfoExRecord> groups,
                                                            int part_num) {
        return new GroupAllocator(groups, part_num);
    }

    public static GroupAllocator buildGroupAllocatorByLocality(String schema, LocalityDesc localityDesc) {
        List<GroupDetailInfoExRecord> groups = getOrderedGroupList(schema);
        if (localityDesc.hasGroupKeyConfig()) {
            groups = groups.stream()
                .filter(x -> localityDesc.matchGroupKey(x.groupName))
                .collect(Collectors.toList());
            groups.sort(Comparator.comparingInt(o -> localityDesc.getGroupKeyList().indexOf(o.groupName)));
        } else {
            groups = groups.stream()
                .filter(x -> localityDesc.matchStorageInstance(x.getStorageInstId()))
                .collect(Collectors.toList());
        }
        return new GroupAllocator(groups);
    }

    public static GroupAllocator buildGroupAllocatorForNonDeletable(String schema) {
        return buildGroupAllocatorByGroup(schema, getOrderedNonDeleteableGroupList(schema));
    }

    /**
     * Get list of storage-group order by physical-table count on the group
     */

    public static List<GroupDetailInfoExRecord> getFullOrderedGroupList(String logicalDbName) {
        if (ConfigDataMode.isMock() || ConfigDataMode.isFastMock()) {
            return TableGroupUtils.mockTheOrderedLocation(logicalDbName);
        }
        List<GroupDetailInfoExRecord> storageGroupList;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            String instId = InstIdUtil.getInstId();
            storageGroupList = queryMetaDbGroupList(conn, instId);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return storageGroupList;
    }

    /**
     * 访问 metadb 操作较重，谨慎使用
     */
    @Deprecated
    public static List<GroupDetailInfoExRecord> getOrderedGroupList(String logicalDbName) {
        return getOrderedGroupList(logicalDbName, false);
    }

    public static List<String> getDNListByDB(String logicalDbName) {
        List<String> dnList = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            String instId = InstIdUtil.getInstId();
            List<GroupDetailInfoExRecord> storageGroupList = queryMetaDbGroupList(conn, instId);
            dnList = storageGroupList.stream().filter(o -> o.dbName.equalsIgnoreCase(logicalDbName))
                .map(o -> o.storageInstId)
                .collect(Collectors.toList());
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return dnList == null ? new ArrayList<>() : dnList;
    }

    /**
     * 访问 metadb 操作较重，谨慎使用
     */
    @Deprecated
    public static List<GroupDetailInfoExRecord> getOrderedGroupList(String logicalDbName,
                                                                    boolean includeToBeRemoveGroup) {
        if (ConfigDataMode.isMock() || ConfigDataMode.isFastMock()) {
            return TableGroupUtils.mockTheOrderedLocation(logicalDbName);
        }

        List<GroupDetailInfoExRecord> storageGroupList;
        List<PartitionGroupExtRecord> partitionGroupList;

        // query metadb for physical group and all partition-groups
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            String instId = InstIdUtil.getInstId();
            storageGroupList = queryMetaDbGroupList(conn, instId);
            partitionGroupList = queryMetaDbPartitionGroupList(logicalDbName, conn);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        // group partitions by storage-instance
        Map<String, String> groupToInstance =
            storageGroupList.stream().collect(Collectors.toMap(x -> x.groupName, x -> x.storageInstId));
        Map<String, Integer> instanceTableCount = new HashMap<>();
        groupToInstance.values().forEach(x -> instanceTableCount.put(x, 0));
        for (PartitionGroupExtRecord partitionGroup : partitionGroupList) {
            String instance = groupToInstance.get(partitionGroup.getGroup_Name());
            instanceTableCount.compute(instance,
                (k, v) -> v == null ? partitionGroup.phy_tb_cnt.intValue() : v + partitionGroup.phy_tb_cnt.intValue());
        }

        // sort physical-groups according to physical-table count
        return storageGroupList.stream()
            .filter(r -> r.dbName.equalsIgnoreCase(logicalDbName))
            .filter(r -> (includeToBeRemoveGroup || DbGroupInfoManager.isNormalGroup(r.dbName,
                r.groupName))) //exclude GROUP_TYPE_BEFORE_REMOVE if includeToBeRemoveGroup=false
            .sorted(Comparator.comparingInt(x -> instanceTableCount.get(x.storageInstId)))
            .collect(Collectors.toList());
    }

    private static List<GroupDetailInfoExRecord> getOrderedNonDeleteableGroupList(String logicalDbName) {
        if (ConfigDataMode.isMock() || ConfigDataMode.isFastMock()) {
            return TableGroupUtils.mockTheOrderedLocation(logicalDbName);
        }

        // query metadb for physical group and all partition-groups
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            String instId = InstIdUtil.getInstId();
            return queryNonDeletableGroupList(conn, instId, logicalDbName);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    private static List<PartitionGroupExtRecord> queryMetaDbPartitionGroupList(String tableSchema, Connection conn) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(conn);
        return partitionGroupAccessor.getGetPhysicalTbCntPerPg(tableSchema);
    }

    private static List<GroupDetailInfoExRecord> queryMetaDbGroupList(Connection conn, String instId) {
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(conn);
        return groupDetailInfoAccessor.getCompletedGroupInfosByInstIdForPartitionTables(instId,
            DbGroupInfoRecord.GROUP_TYPE_NORMAL);
    }

    private static List<GroupDetailInfoExRecord> queryNonDeletableGroupList(Connection conn,
                                                                            String instId,
                                                                            String schema) {
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(conn);
        return groupDetailInfoAccessor.getCompletedGroupInfosBySchemaForNonDeletable(instId,
            schema,
            DbGroupInfoRecord.GROUP_TYPE_NORMAL);
    }

    /**
     * Allocate group from a list of groups, use round-robin strategy
     */
    public static class GroupAllocator {
        private final List<GroupDetailInfoExRecord> groupList;
        private int nextToAllocate;

        private int partNum;
        private int avgPartNum;

        private int allocatedCount;

        public boolean isAllocateByGroup() {
            return allocateByGroup;
        }

        private boolean allocateByGroup;

        GroupAllocator(List<GroupDetailInfoExRecord> groupList) {
            this.groupList = groupList;
            this.nextToAllocate = 0;
            this.partNum = 0;
            this.allocateByGroup = false;
        }

        GroupAllocator(List<GroupDetailInfoExRecord> groupList, int partNum) {
            this.groupList = groupList;
            this.nextToAllocate = 0;
            this.partNum = partNum;
            this.avgPartNum = Math.max(partNum / groupList.size(), 1);
            this.allocatedCount = 0;
            this.allocateByGroup = true;
        }

        /**
         * Allocate a group
         */
        public String allocate() {
            String result = this.groupList.get(nextToAllocate).getGroupName();
            if (partNum == 0) {
                this.nextToAllocate = (this.nextToAllocate + 1) % this.groupList.size();
            } else {
                //Sequential count
                allocatedCount += 1;
                if (allocatedCount % avgPartNum == 0) {
                    this.nextToAllocate = (this.nextToAllocate + 1) % this.groupList.size();
                }
            }
            return result;
        }
    }

}

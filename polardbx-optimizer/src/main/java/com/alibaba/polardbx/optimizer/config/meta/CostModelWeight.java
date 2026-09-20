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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.config.meta.CostModel.CostModelWeightService;
import com.alibaba.polardbx.optimizer.config.meta.CostModel.ICostModelWeightService;
import com.alibaba.polardbx.optimizer.config.meta.CostModel.ImmutableCostModelWeightV1;
import com.alibaba.polardbx.optimizer.config.meta.CostModel.ImmutableCostModelWeightV2;
import com.google.common.collect.ImmutableMap;

public class CostModelWeight {
    public static final String EARLIEST = "V1";
    public static final String LATEST_KEY = "LATEST";

    private static String CURRENT = EARLIEST;
    private static final ImmutableMap<String, ICostModelWeightService> versionMap =
        ImmutableMap.<String, ICostModelWeightService>builder()
            .put("V1", new ImmutableCostModelWeightV1())
            .put("V2", new ImmutableCostModelWeightV2())
            .build();

    public static final CostModelWeightService INSTANCE = new CostModelWeightService(versionMap.get(EARLIEST));

    public static void setVersion(String version) {
        String targetVersion = StringUtils.isEmpty(version) ?
            PropUtil.COST_MODEL_LATEST : version.toUpperCase();
        if (LATEST_KEY.equals(targetVersion)) {
            targetVersion = PropUtil.COST_MODEL_LATEST;
        }
        if (!versionMap.containsKey(targetVersion)) {
            targetVersion = PropUtil.COST_MODEL_LATEST;
        }
        if (!targetVersion.equals(CURRENT)) {
            synchronized (INSTANCE) {
                if (!targetVersion.equals(CURRENT)) {
                    CURRENT = targetVersion;
                    INSTANCE.setImmutable(versionMap.get(targetVersion));
                }
            }
        }
    }

    public static double SINGLETON_CPU_COST = 0.125;

    public static double RANDOM_CPU_COST = 0.125;

    public static double ROUND_ROBIN_CPU_COST = 0.125;

    public static double RANGE_PARTITION_CPU_COST = 3;

    public static double HASH_CPU_COST = 2;

    public static double SERIALIZE_DESERIALIZE_CPU_COST = 10;

    public static double COLUMNAR_EXCHANGE_FACTOR = 0.5;

    public static final double SEQ_IO_PAGE_SIZE = 32 * 1024;

    public static final double RAND_IO_PAGE_SIZE = 4 * 1024;

    public static final double OSS_PAGE_SIZE = 1000;

    public static final double OSS_ROW_GROUP_SIZE = 10000;

    public static final double OSS_COLUMN_DECOMPRESS_WEIGHT = 0.1;

    public static final double OSS_ROW_GROUP_FANOUT = 2;

    public static final double OSS_MAX_ROWS_PER_FILE = 1000 * 1000;

    public static final double BLOOM_FILTER_READ_COST = 10;

    public static final double NET_BUFFER_SIZE = 8 * 1024 * 1024;

    public static final double WORKLOAD_MEMORY_WEIGHT = 10;

    public static final double CPU_START_UP_COST = 10;

    public static final double LOOKUP_NUM_PER_IO = 1;

    public static final long TUPLE_HEADER_SIZE = 24;

    public static final int LOOKUP_START_UP_NET = 12;

    public static final int GUESS_AGG_OUTPUT_NUM = 100;
}

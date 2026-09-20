/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.table;

public class BlockChainHistoryTable {
    /**
     * 加入原表名
     */
    public static String tableNameFormat = "__%s_hist";
    /**
     * 加入原库名，表名
     */
    public static String createTableNameFormat = "CREATE TABLE `%s`.`%s` (\n" +
        "        `block_id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
        "        `trace_id` varchar(64),\n" +
        "        `start_time` timestamp NOT NULL,\n" +
        "        `rec_num` bigint(20) DEFAULT '0',\n" +
        "        `op_type` varchar(64),\n" +
        "        `hash_ins` char(64),\n" +
        "        `hash_del` char(64),\n" +
        "        `block_hash` char(64),\n" +
        "        `long_pk` bigint(20) DEFAULT NULL,\n" +
        "        `bytes_pk` blob DEFAULT NULL,\n" +
        "        `tso` bigint(20) DEFAULT NULL,\n" +
        "        `extra` longtext DEFAULT NULL,\n" +
        "        `gmt_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
        "        `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
        "        PRIMARY KEY (`block_id`),\n" +
        "        KEY `idx_trace_id_rec_num`(`trace_id`, `rec_num`),\n" +
        "        KEY `idx_tso`(`tso`)\n" +
        "    ) DEFAULT CHARSET = utf8mb4 PARTITION BY KEY(`block_id`) PARTITIONS 16; ";
}

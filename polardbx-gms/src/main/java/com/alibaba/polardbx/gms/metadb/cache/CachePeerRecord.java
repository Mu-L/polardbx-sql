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

package com.alibaba.polardbx.gms.metadb.cache;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CachePeerRecord implements SystemTableRecord {
    public long id;
    public String peerName;
    public String host;
    public long nodeHash;
    public long lease;
    public String role;
    public boolean leader;
    public String statusJson;

    @Override
    public CachePeerRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.peerName = rs.getString("peer_name");
        this.host = rs.getString("host");
        this.nodeHash = rs.getLong("node_hash");
        this.lease = rs.getLong("lease");
        this.role = rs.getString("role");
        this.leader = rs.getBoolean("leader");
        this.statusJson = rs.getString("status_json");
        return this;
    }
}

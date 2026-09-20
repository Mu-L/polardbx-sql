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

import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CacheRecordTest {

    @Mock
    ResultSet rs;

    @Test
    public void testSystemTableRegistered() {
        Assert.assertTrue(GmsSystemTables.contains("cache_user"));
        Assert.assertTrue(GmsSystemTables.contains("cache_peer"));
        Assert.assertTrue(GmsSystemTables.contains("cache_file_mapping"));
    }

    @Test
    public void testCacheUserRecordFill() throws SQLException {
        when(rs.getLong(matches("id"))).thenReturn(1L);
        when(rs.getString(matches("user_name"))).thenReturn("test_user");
        when(rs.getString(matches("password"))).thenReturn("enc_password");
        when(rs.getInt(matches("read_priv"))).thenReturn(1);
        when(rs.getInt(matches("write_priv"))).thenReturn(0);
        when(rs.getInt(matches("admin_priv"))).thenReturn(1);

        CacheUserRecord record = new CacheUserRecord().fill(rs);
        Assert.assertEquals(1L, record.id);
        Assert.assertEquals("test_user", record.userName);
        Assert.assertEquals("enc_password", record.password);
        Assert.assertEquals(1, record.readPriv);
        Assert.assertEquals(0, record.writePriv);
        Assert.assertEquals(1, record.adminPriv);
    }

    @Test
    public void testCachePeerRecordFill() throws SQLException {
        when(rs.getLong(matches("id"))).thenReturn(10L);
        when(rs.getString(matches("peer_name"))).thenReturn("node-1");
        when(rs.getString(matches("host"))).thenReturn("192.168.1.1:8080");
        when(rs.getLong(matches("node_hash"))).thenReturn(123456789L);
        when(rs.getLong(matches("lease"))).thenReturn(9999999999L);
        when(rs.getString(matches("role"))).thenReturn("CACHE_WRITER");
        when(rs.getBoolean(matches("leader"))).thenReturn(true);

        CachePeerRecord record = new CachePeerRecord().fill(rs);
        Assert.assertEquals(10L, record.id);
        Assert.assertEquals("node-1", record.peerName);
        Assert.assertEquals("192.168.1.1:8080", record.host);
        Assert.assertEquals(123456789L, record.nodeHash);
        Assert.assertEquals(9999999999L, record.lease);
        Assert.assertEquals("CACHE_WRITER", record.role);
        Assert.assertTrue(record.leader);
    }

    @Test
    public void testCacheFileMappingRecordFill() throws SQLException {
        when(rs.getLong(matches("id"))).thenReturn(42L);
        when(rs.getTimestamp(matches("gmt_created"))).thenReturn(new java.sql.Timestamp(1000L));
        when(rs.getTimestamp(matches("gmt_modified"))).thenReturn(new java.sql.Timestamp(2000L));
        when(rs.getString(matches("file_name"))).thenReturn("test.orc");
        when(rs.getString(matches("logical_schema"))).thenReturn("my_db");
        when(rs.getString(matches("logical_table"))).thenReturn("my_table");
        when(rs.getString(matches("part_name"))).thenReturn("p0");
        when(rs.getString(matches("engine"))).thenReturn("columnar");
        when(rs.getBytes(matches("meta"))).thenReturn(new byte[] {1, 2, 3});
        when(rs.getLong(matches("version"))).thenReturn(5L);
        when(rs.getBoolean(matches("locked"))).thenReturn(true);

        CacheFileMappingRecord record = new CacheFileMappingRecord().fill(rs);
        Assert.assertEquals(42L, record.id);
        Assert.assertNotNull(record.gmtCreated);
        Assert.assertNotNull(record.gmtModified);
        Assert.assertEquals("test.orc", record.fileName);
        Assert.assertEquals("my_db", record.logicalSchema);
        Assert.assertEquals("my_table", record.logicalTable);
        Assert.assertEquals("p0", record.partName);
        Assert.assertEquals("columnar", record.engine);
        Assert.assertArrayEquals(new byte[] {1, 2, 3}, record.meta);
        Assert.assertEquals(5L, record.version);
        Assert.assertTrue(record.locked);
    }
}

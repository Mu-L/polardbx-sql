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
package com.alibaba.polardbx.gms.metadb.model;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Map;

public class SkillRecordTest {

    // ==================== SkillConfigRecord ====================

    @Test
    public void testSkillConfigRecord_fill() throws Exception {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getLong("id")).thenReturn(1L);
        Mockito.when(rs.getString("name")).thenReturn("test-skill");
        Mockito.when(rs.getString("description")).thenReturn("desc");
        Mockito.when(rs.getString("prompt")).thenReturn("prompt");
        Mockito.when(rs.getString("status")).thenReturn("ACTIVE");
        Mockito.when(rs.getInt("priority")).thenReturn(10);
        Mockito.when(rs.getInt("builtin")).thenReturn(1);
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        Mockito.when(rs.getTimestamp("gmt_created")).thenReturn(ts);
        Mockito.when(rs.getTimestamp("gmt_modified")).thenReturn(ts);

        SkillConfigRecord record = new SkillConfigRecord();
        record.fill(rs);

        Assert.assertEquals(1L, record.id);
        Assert.assertEquals("test-skill", record.name);
        Assert.assertEquals("desc", record.description);
        Assert.assertEquals("prompt", record.prompt);
        Assert.assertEquals("ACTIVE", record.status);
        Assert.assertEquals(10, record.priority);
        Assert.assertEquals(1, record.builtin);
        Assert.assertEquals(ts, record.gmtCreated);
        Assert.assertEquals(ts, record.gmtModified);
    }

    @Test
    public void testSkillConfigRecord_buildInsertParams() {
        SkillConfigRecord record = new SkillConfigRecord();
        record.name = "skill1";
        record.description = "desc1";
        record.prompt = "prompt1";
        record.status = "ACTIVE";
        record.priority = 5;
        record.builtin = 0;

        Map<Integer, ParameterContext> params = record.buildInsertParams();
        Assert.assertEquals(6, params.size());
        Assert.assertEquals("skill1", params.get(1).getValue());
        Assert.assertEquals("desc1", params.get(2).getValue());
        Assert.assertEquals("prompt1", params.get(3).getValue());
        Assert.assertEquals("ACTIVE", params.get(4).getValue());
        Assert.assertEquals(5, params.get(5).getValue());
        Assert.assertEquals(0, params.get(6).getValue());
    }

    // ==================== SkillReferenceRecord ====================

    @Test
    public void testSkillReferenceRecord_fill() throws Exception {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getLong("id")).thenReturn(2L);
        Mockito.when(rs.getString("skill_name")).thenReturn("skill1");
        Mockito.when(rs.getString("ref_name")).thenReturn("ref1");
        Mockito.when(rs.getString("content")).thenReturn("content1");
        Mockito.when(rs.getInt("priority")).thenReturn(20);
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        Mockito.when(rs.getTimestamp("gmt_created")).thenReturn(ts);
        Mockito.when(rs.getTimestamp("gmt_modified")).thenReturn(ts);

        SkillReferenceRecord record = new SkillReferenceRecord();
        record.fill(rs);

        Assert.assertEquals(2L, record.id);
        Assert.assertEquals("skill1", record.skillName);
        Assert.assertEquals("ref1", record.refName);
        Assert.assertEquals("content1", record.content);
        Assert.assertEquals(20, record.priority);
        Assert.assertEquals(ts, record.gmtCreated);
        Assert.assertEquals(ts, record.gmtModified);
    }

    @Test
    public void testSkillReferenceRecord_buildInsertParams() {
        SkillReferenceRecord record = new SkillReferenceRecord();
        record.skillName = "skill1";
        record.refName = "ref1";
        record.content = "content1";
        record.priority = 15;

        Map<Integer, ParameterContext> params = record.buildInsertParams();
        Assert.assertEquals(4, params.size());
        Assert.assertEquals("skill1", params.get(1).getValue());
        Assert.assertEquals("ref1", params.get(2).getValue());
        Assert.assertEquals("content1", params.get(3).getValue());
        Assert.assertEquals(15, params.get(4).getValue());
    }
}

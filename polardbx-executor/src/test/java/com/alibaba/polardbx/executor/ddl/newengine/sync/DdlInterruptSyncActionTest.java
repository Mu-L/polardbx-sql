package com.alibaba.polardbx.executor.ddl.newengine.sync;

import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DdlInterruptSyncActionTest {

    @Test
    public void testSerializeAndDeserialize() {
        String schemaName = "test_schema";
        List<Long> jobIds = Arrays.asList(1L, 2L, 3L);
        Boolean pauseElseTransition = false;

        DdlRequest ddlRequest = new DdlRequest(schemaName, jobIds);

        DdlInterruptSyncAction originalAction = new DdlInterruptSyncAction(ddlRequest, pauseElseTransition);

        String jsonString = JSON.toJSONString(originalAction);

        DdlInterruptSyncAction deserializedAction = JSON.parseObject(jsonString, DdlInterruptSyncAction.class);

        // 验证成员变量一致性
        assertEquals(originalAction.getPauseElseTransition(), deserializedAction.getPauseElseTransition());

        DdlRequest originalRequest = originalAction.getDdlRequest();
        DdlRequest deserializedRequest = deserializedAction.getDdlRequest();

        if (originalRequest == null) {
            assertNull(deserializedRequest);
        } else {
            assertEquals(originalRequest.getSchemaName(), deserializedRequest.getSchemaName());
            assertEquals(originalRequest.getJobIds(), deserializedRequest.getJobIds());
        }
    }

    @Test
    public void testSerializeAndDeserializeWithNullValues() {
        // 创建DdlInterruptSyncAction，使用默认构造函数
        DdlInterruptSyncAction originalAction = new DdlInterruptSyncAction();

        String jsonString = JSON.toJSONString(originalAction);

        DdlInterruptSyncAction deserializedAction = JSON.parseObject(jsonString, DdlInterruptSyncAction.class);

        // 验证成员变量一致性
        assertEquals(originalAction.getPauseElseTransition(), deserializedAction.getPauseElseTransition());

        DdlRequest originalRequest = originalAction.getDdlRequest();
        DdlRequest deserializedRequest = deserializedAction.getDdlRequest();

        if (originalRequest == null) {
            assertNull(deserializedRequest);
        } else {
            assertEquals(originalRequest.getSchemaName(), deserializedRequest.getSchemaName());
            assertEquals(originalRequest.getJobIds(), deserializedRequest.getJobIds());
        }
    }

    @Test
    public void testSerializeAndDeserializeWithNullRequest() {
        // 创建DdlInterruptSyncAction，request为null
        Boolean pauseElseTransition = true;

        DdlInterruptSyncAction originalAction = new DdlInterruptSyncAction(null, pauseElseTransition);

        String jsonString = JSON.toJSONString(originalAction);

        DdlInterruptSyncAction deserializedAction = JSON.parseObject(jsonString, DdlInterruptSyncAction.class);

        // 验证成员变量一致性
        assertEquals(originalAction.getPauseElseTransition(), deserializedAction.getPauseElseTransition());

        DdlRequest originalRequest = originalAction.getDdlRequest();
        DdlRequest deserializedRequest = deserializedAction.getDdlRequest();

        if (originalRequest == null) {
            assertNull(deserializedRequest);
        } else {
            assertEquals(originalRequest.getSchemaName(), deserializedRequest.getSchemaName());
            assertEquals(originalRequest.getJobIds(), deserializedRequest.getJobIds());
        }
    }
}
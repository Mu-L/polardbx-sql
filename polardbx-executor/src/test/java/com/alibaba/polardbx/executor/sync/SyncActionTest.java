package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SyncActionTest {

    @Test
    public void testFailPointEnableSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FailPointEnableSyncAction");

        String key = "SyncActionTestKey";
        FailPointEnableSyncAction action = new FailPointEnableSyncAction(key, "true");
        action.setFpKey(key);
        action.setValue("true");

        // serialize
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        // deserialize
        FailPointEnableSyncAction action1 = (FailPointEnableSyncAction) JSON.parse(data);

        action1.sync();
        Assert.assertTrue(FailPoint.isKeyEnable(key));

        // clear
        FailPoint.disable(key);
    }

    @Test
    public void testFailPointDisableSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FailPointDisableSyncAction");

        String key = "SyncActionTestKey";
        FailPointDisableSyncAction action = new FailPointDisableSyncAction(key);
        action.setFpKey(key);

        // serialize
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        // deserialize
        FailPointDisableSyncAction action1 = (FailPointDisableSyncAction) JSON.parse(data);

        FailPoint.enable(key, "true");
        action1.sync();
        Assert.assertTrue(!FailPoint.isKeyEnable(key));
    }

    @Test
    public void testFailPointClearSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FailPointClearSyncAction");

        String key = "SyncActionTestKey";
        FailPointClearSyncAction action = new FailPointClearSyncAction();

        // serialize
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        // deserialize
        FailPointClearSyncAction action1 = (FailPointClearSyncAction) JSON.parse(data);

        FailPoint.enable(key, "true");
        action1.sync();
        Assert.assertTrue(!FailPoint.isKeyEnable(key));
    }

    @Test
    public void testDestroyPhysicalBackfillDataSourcesSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        parserConfig.addAccept(
            "com.alibaba.polardbx.executor.sync.DestroyPhysicalBackfillDataSourcesSyncAction");

        Long rootJobId = 123L;
        DestroyPhysicalBackfillDataSourcesSyncAction action =
            new DestroyPhysicalBackfillDataSourcesSyncAction(rootJobId);
        org.junit.Assert.assertEquals(rootJobId, action.getRootJobId());

        DestroyPhysicalBackfillDataSourcesSyncAction actionWithSetter =
            new DestroyPhysicalBackfillDataSourcesSyncAction();
        actionWithSetter.setRootJobId(rootJobId);
        org.junit.Assert.assertEquals(rootJobId, actionWithSetter.getRootJobId());

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        DestroyPhysicalBackfillDataSourcesSyncAction deserializedAction =
            (DestroyPhysicalBackfillDataSourcesSyncAction) JSON.parse(data);
        org.junit.Assert.assertEquals(rootJobId, deserializedAction.getRootJobId());

        try (MockedStatic<PhysicalBackfillUtils> mockedUtils =
            Mockito.mockStatic(PhysicalBackfillUtils.class)) {
            org.junit.Assert.assertNull(deserializedAction.sync());
            mockedUtils.verify(
                () -> PhysicalBackfillUtils.destroyDataSources(rootJobId), Mockito.times(1));
        }
    }
}

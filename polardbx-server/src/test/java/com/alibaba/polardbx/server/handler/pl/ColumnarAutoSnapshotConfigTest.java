package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.handler.pl.inner.ColumnarAutoSnapshotConfigProcedure;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.TddlConstants.COLUMNAR_AUTO_SNAPSHOT_CONFIG;
import static com.alibaba.polardbx.common.TddlConstants.CRON_EXPR;
import static com.alibaba.polardbx.common.TddlConstants.ZONE_ID;
import static com.cronutils.model.CronType.QUARTZ;

public class ColumnarAutoSnapshotConfigTest {
    @Test
    public void testNoConfigShow() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);
            List<ColumnarConfigRecord> columnarConfigRecords = ImmutableList.of();
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(columnarConfigRecords);
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_auto_snapshot_config('SHOW')",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
            ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
            // Execute inner procedure.
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals("No config found.", cursor.getRows().get(0).getString(0));
            System.out.println(cursor);
        }
    }

    @Test
    public void testHasConfigShow() {
        // Existed config.
        CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        Cron cron = quartzCronParser.parse("0 0 * * * ?");
        ZoneId zoneId = TimeZoneUtils.zoneIdOf("+08:00");
        Map<String, String> config = new HashMap<>();
        config.put(CRON_EXPR, cron.asString());
        config.put(ZONE_ID, zoneId.getId());
        String configStr = JSON.toJSONString(config);
        ColumnarConfigRecord record = new ColumnarConfigRecord();
        record.configKey = COLUMNAR_AUTO_SNAPSHOT_CONFIG;
        record.configValue = configStr;

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);
            List<ColumnarConfigRecord> columnarConfigRecords = ImmutableList.of(record);
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(columnarConfigRecords);
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_auto_snapshot_config(SHOW)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
            ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
            // Execute inner procedure.
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals("Auto snapshot config: cron expression: 0 0 * * * ? zone id: +08:00",
                cursor.getRows().get(0).getString(0));
            System.out.println(cursor);
        }
    }

    @Test
    public void testNoConfigEnable() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);
            List<ColumnarConfigRecord> columnarConfigRecords = ImmutableList.of();
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(columnarConfigRecords);
            metaDbUtilMock.when(() -> MetaDbUtil.insert(Mockito.any(), Mockito.anyList(), Mockito.any()))
                .thenReturn(new int[] {1});
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_auto_snapshot_config('ENABLE', '0 0 * * * ?', '+08:00')",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
            ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
            // Execute inner procedure.
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(3, cursor.getRows().size());
            Assert.assertEquals("Enable new auto generated snapshot.",
                cursor.getRows().get(0).getString(0));
            Assert.assertEquals("Before config: null",
                cursor.getRows().get(1).getString(0));
            Assert.assertEquals("Current config: cron expression: 0 0 * * * ? zone id: +08:00",
                cursor.getRows().get(2).getString(0));
            System.out.println(cursor);
        }
    }

    @Test
    public void testHasConfigEnable() {
        // Existed config.
        CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        Cron cron = quartzCronParser.parse("0 1 * * * ?");
        ZoneId zoneId = TimeZoneUtils.zoneIdOf("+08:00");
        Map<String, String> config = new HashMap<>();
        config.put(CRON_EXPR, cron.asString());
        config.put(ZONE_ID, zoneId.getId());
        String configStr = JSON.toJSONString(config);
        ColumnarConfigRecord record = new ColumnarConfigRecord();
        record.configKey = COLUMNAR_AUTO_SNAPSHOT_CONFIG;
        record.configValue = configStr;

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);
            List<ColumnarConfigRecord> columnarConfigRecords = ImmutableList.of(record);
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(columnarConfigRecords);
            metaDbUtilMock.when(() -> MetaDbUtil.insert(Mockito.any(), Mockito.anyList(), Mockito.any()))
                .thenReturn(new int[] {1});
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_auto_snapshot_config('ENABLE', '0 0 * * * ?', '+08:00')",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
            ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
            // Execute inner procedure.
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(3, cursor.getRows().size());
            Assert.assertEquals("Enable new auto generated snapshot.",
                cursor.getRows().get(0).getString(0));
            Assert.assertEquals("Before config: cron expression: 0 1 * * * ? zone id: +08:00",
                cursor.getRows().get(1).getString(0));
            Assert.assertEquals("Current config: cron expression: 0 0 * * * ? zone id: +08:00",
                cursor.getRows().get(2).getString(0));
            System.out.println(cursor);
        }
    }

    @Test
    public void testNoConfigDisable() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);
            List<ColumnarConfigRecord> columnarConfigRecords = ImmutableList.of();
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(columnarConfigRecords);
            metaDbUtilMock.when(() -> MetaDbUtil.insert(Mockito.any(), Mockito.anyList(), Mockito.any()))
                .thenReturn(new int[] {1});
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_auto_snapshot_config('DISABLE')",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
            ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
            // Execute inner procedure.
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(2, cursor.getRows().size());
            Assert.assertEquals("Disable auto generated snapshot.",
                cursor.getRows().get(0).getString(0));
            Assert.assertEquals("Before config: null",
                cursor.getRows().get(1).getString(0));
            System.out.println(cursor);
        }
    }

    @Test
    public void testHasConfigDisable() {
        // Existed config.
        CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        Cron cron = quartzCronParser.parse("0 1 * * * ?");
        ZoneId zoneId = TimeZoneUtils.zoneIdOf("+08:00");
        Map<String, String> config = new HashMap<>();
        config.put(CRON_EXPR, cron.asString());
        config.put(ZONE_ID, zoneId.getId());
        String configStr = JSON.toJSONString(config);
        ColumnarConfigRecord record = new ColumnarConfigRecord();
        record.configKey = COLUMNAR_AUTO_SNAPSHOT_CONFIG;
        record.configValue = configStr;

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);
            List<ColumnarConfigRecord> columnarConfigRecords = ImmutableList.of(record);
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(columnarConfigRecords);
            metaDbUtilMock.when(() -> MetaDbUtil.insert(Mockito.any(), Mockito.anyList(), Mockito.any()))
                .thenReturn(new int[] {1});
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_auto_snapshot_config('DISABLE')",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
            ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
            // Execute inner procedure.
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(2, cursor.getRows().size());
            Assert.assertEquals("Disable auto generated snapshot.",
                cursor.getRows().get(0).getString(0));
            Assert.assertEquals("Before config: cron expression: 0 1 * * * ? zone id: +08:00",
                cursor.getRows().get(1).getString(0));
            System.out.println(cursor);
        }
    }

    @Test
    public void testErrorInput() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);
            List<ColumnarConfigRecord> columnarConfigRecords = ImmutableList.of();
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(columnarConfigRecords);
            // Execute inner procedure.
            try {
                SQLCallStatement statement =
                    (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_auto_snapshot_config()",
                        SQLParserFeature.IgnoreNameQuotes).get(0);
                ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
                ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
                procedure.execute(null, statement, cursor);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Bad arguments."));
            }

            try {
                SQLCallStatement statement =
                    (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_auto_snapshot_config(SHWO)",
                        SQLParserFeature.IgnoreNameQuotes).get(0);
                ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
                ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
                procedure.execute(null, statement, cursor);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Bad arguments."));
            }

            try {
                SQLCallStatement statement =
                    (SQLCallStatement) FastsqlUtils.parseSql(
                        "call polardbx.columnar_auto_snapshot_config('ENABLE', '', '')",
                        SQLParserFeature.IgnoreNameQuotes).get(0);
                ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
                ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
                procedure.execute(null, statement, cursor);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Empty expression!"));
            }

            try {
                SQLCallStatement statement =
                    (SQLCallStatement) FastsqlUtils.parseSql(
                        "call polardbx.columnar_auto_snapshot_config('ENABLE', '')",
                        SQLParserFeature.IgnoreNameQuotes).get(0);
                ArrayResultCursor cursor = new ArrayResultCursor("columnar_auto_snapshot_config");
                ColumnarAutoSnapshotConfigProcedure procedure = new ColumnarAutoSnapshotConfigProcedure();
                procedure.execute(null, statement, cursor);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Bad arguments."));
            }
        }
    }
}

package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.common.SQLModeFlags;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.sql.Types;
import java.time.ZoneId;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class for DatetimeField new features added in commits:
 * - add new sqlmode && add TimeAdjustFractionUtil
 * - add truncate path in storeMysqlDatetime (two commits)
 * <p>
 * This test covers all modified and new lines in the three commits.
 */
@RunWith(MockitoJUnitRunner.class)
public class DatetimeFieldNewFeaturesTest {

    private DatetimeField datetimeField;
    private DataType<?> fieldType;
    private SessionProperties sessionProperties;

    @Before
    public void setUp() {
        fieldType = new DateTimeType(3); // scale = 3 for testing fractional seconds
        datetimeField = new DatetimeField(fieldType);

        sessionProperties = new SessionProperties(
            ZoneId.systemDefault(),
            CharsetName.defaultCharset(),
            0L, // will be modified in individual tests
            FieldCheckLevel.CHECK_FIELD_WARN
        );
    }

    /**
     * Test dateFlags method - covers the new line adding MODE_TIME_TRUNCATE_FRACTIONAL check
     */
    @Test
    public void testDateFlagsWithTruncateFractional() throws Exception {
        // Test with MODE_TIME_TRUNCATE_FRACTIONAL flag set
        SessionProperties sessionPropsWithTruncate = new SessionProperties(
            ZoneId.systemDefault(),
            CharsetName.defaultCharset(),
            SQLModeFlags.MODE_TIME_TRUNCATE_FRACTIONAL,
            FieldCheckLevel.CHECK_FIELD_WARN
        );

        // Use reflection to access the private dateFlags method
        Method dateFlags = DatetimeField.class.getDeclaredMethod("dateFlags", SessionProperties.class);
        dateFlags.setAccessible(true);

        int flags = (Integer) dateFlags.invoke(datetimeField, sessionPropsWithTruncate);

        // Verify that FLAG_TIME_TRUNCATE_FRACTIONAL is included in the flags
        assertTrue("Should include TIME_TRUNCATE_FRACTIONAL flag",
            (flags & TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL) != 0);
    }

    /**
     * Test dateFlags method without truncate fractional mode
     */
    @Test
    public void testDateFlagsWithoutTruncateFractional() throws Exception {
        // Test without MODE_TIME_TRUNCATE_FRACTIONAL flag
        SessionProperties sessionPropsWithoutTruncate = new SessionProperties(
            ZoneId.systemDefault(),
            CharsetName.defaultCharset(),
            0L, // no special flags
            FieldCheckLevel.CHECK_FIELD_WARN
        );

        Method dateFlags = DatetimeField.class.getDeclaredMethod("dateFlags", SessionProperties.class);
        dateFlags.setAccessible(true);

        int flags = (Integer) dateFlags.invoke(datetimeField, sessionPropsWithoutTruncate);

        // Verify that FLAG_TIME_TRUNCATE_FRACTIONAL is NOT included in the flags
        assertFalse("Should not include TIME_TRUNCATE_FRACTIONAL flag",
            (flags & TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL) != 0);
    }

    /**
     * Test storeMysqlDatetime method with MySQL 8.0 and truncate fractional mode
     * Covers the new version check and MySQLTimeCalculator.timeTruncate call
     */
    @Test
    public void testStoreMysqlDatetimeWithMySQL80AndTruncate() throws Exception {
        try (MockedStatic<InstanceVersion> instanceVersionMock = mockStatic(InstanceVersion.class);
            MockedStatic<MySQLTimeCalculator> calculatorMock = mockStatic(MySQLTimeCalculator.class)) {

            // Mock MySQL 8.0
            instanceVersionMock.when(InstanceVersion::isMYSQL80).thenReturn(true);

            // Setup session properties with truncate fractional mode
            SessionProperties sessionPropsWithTruncate = new SessionProperties(
                ZoneId.systemDefault(),
                CharsetName.defaultCharset(),
                SQLModeFlags.MODE_TIME_TRUNCATE_FRACTIONAL,
                FieldCheckLevel.CHECK_FIELD_WARN
            );

            // Create a test MysqlDateTime
            MysqlDateTime mysqlDateTime = new MysqlDateTime();
            mysqlDateTime.setYear(2023);
            mysqlDateTime.setMonth(7);
            mysqlDateTime.setDay(25);
            mysqlDateTime.setHour(10);
            mysqlDateTime.setMinute(30);
            mysqlDateTime.setSecond(45);
            mysqlDateTime.setSecondPart(123456L); // microseconds
            mysqlDateTime.setSqlType(Types.TIMESTAMP);

            // Use reflection to access the private storeMysqlDatetime method
            Method storeMysqlDatetime = DatetimeField.class.getDeclaredMethod(
                "storeMysqlDatetime", MysqlDateTime.class, SessionProperties.class);
            storeMysqlDatetime.setAccessible(true);

            // Call the method
            TypeConversionStatus result = (TypeConversionStatus) storeMysqlDatetime.invoke(
                datetimeField, mysqlDateTime, sessionPropsWithTruncate);

            // Verify that InstanceVersion.isMYSQL80() was called
            instanceVersionMock.verify(InstanceVersion::isMYSQL80, times(1));

            // Verify that MySQLTimeCalculator.timeTruncate was called with the cloned datetime
            calculatorMock.verify(() -> MySQLTimeCalculator.timeTruncate(any(MysqlDateTime.class), eq(3)), times(1));

            assertEquals("Should return TYPE_OK", TypeConversionStatus.TYPE_OK, result);
        }
    }

    /**
     * Test storeMysqlDatetime method with non-MySQL 8.0 - should not call truncate
     */
    @Test
    public void testStoreMysqlDatetimeWithNonMySQL80() throws Exception {
        try (MockedStatic<InstanceVersion> instanceVersionMock = mockStatic(InstanceVersion.class);
            MockedStatic<MySQLTimeCalculator> calculatorMock = mockStatic(MySQLTimeCalculator.class)) {

            // Mock non-MySQL 8.0
            instanceVersionMock.when(InstanceVersion::isMYSQL80).thenReturn(false);

            SessionProperties sessionPropsWithTruncate = new SessionProperties(
                ZoneId.systemDefault(),
                CharsetName.defaultCharset(),
                SQLModeFlags.MODE_TIME_TRUNCATE_FRACTIONAL,
                FieldCheckLevel.CHECK_FIELD_WARN
            );

            MysqlDateTime mysqlDateTime = new MysqlDateTime();
            mysqlDateTime.setYear(2023);
            mysqlDateTime.setMonth(7);
            mysqlDateTime.setDay(25);
            mysqlDateTime.setHour(10);
            mysqlDateTime.setMinute(30);
            mysqlDateTime.setSecond(45);
            mysqlDateTime.setSecondPart(123456L);
            mysqlDateTime.setSqlType(Types.TIMESTAMP);

            Method storeMysqlDatetime = DatetimeField.class.getDeclaredMethod(
                "storeMysqlDatetime", MysqlDateTime.class, SessionProperties.class);
            storeMysqlDatetime.setAccessible(true);

            TypeConversionStatus result = (TypeConversionStatus) storeMysqlDatetime.invoke(
                datetimeField, mysqlDateTime, sessionPropsWithTruncate);

            // Verify that InstanceVersion.isMYSQL80() was called
            instanceVersionMock.verify(InstanceVersion::isMYSQL80, times(1));

            // Verify that MySQLTimeCalculator.timeTruncate was NOT called
            calculatorMock.verify(() -> MySQLTimeCalculator.timeTruncate(any(MysqlDateTime.class), anyInt()), never());

            assertEquals("Should return TYPE_OK", TypeConversionStatus.TYPE_OK, result);
        }
    }

    /**
     * Test storeMysqlDatetime method with MySQL 8.0 but without truncate fractional mode
     */
    @Test
    public void testStoreMysqlDatetimeWithMySQL80WithoutTruncateMode() throws Exception {
        try (MockedStatic<InstanceVersion> instanceVersionMock = mockStatic(InstanceVersion.class);
            MockedStatic<MySQLTimeCalculator> calculatorMock = mockStatic(MySQLTimeCalculator.class)) {

            // Mock MySQL 8.0
            instanceVersionMock.when(InstanceVersion::isMYSQL80).thenReturn(true);

            // Session properties WITHOUT truncate fractional mode
            SessionProperties sessionPropsWithoutTruncate = new SessionProperties(
                ZoneId.systemDefault(),
                CharsetName.defaultCharset(),
                0L, // no truncate flag
                FieldCheckLevel.CHECK_FIELD_WARN
            );

            MysqlDateTime mysqlDateTime = new MysqlDateTime();
            mysqlDateTime.setYear(2023);
            mysqlDateTime.setMonth(7);
            mysqlDateTime.setDay(25);
            mysqlDateTime.setHour(10);
            mysqlDateTime.setMinute(30);
            mysqlDateTime.setSecond(45);
            mysqlDateTime.setSecondPart(123456L);
            mysqlDateTime.setSqlType(Types.TIMESTAMP);

            Method storeMysqlDatetime = DatetimeField.class.getDeclaredMethod(
                "storeMysqlDatetime", MysqlDateTime.class, SessionProperties.class);
            storeMysqlDatetime.setAccessible(true);

            TypeConversionStatus result = (TypeConversionStatus) storeMysqlDatetime.invoke(
                datetimeField, mysqlDateTime, sessionPropsWithoutTruncate);

            // Verify that InstanceVersion.isMYSQL80() was called
            instanceVersionMock.verify(InstanceVersion::isMYSQL80, times(1));

            // Verify that MySQLTimeCalculator.timeTruncate was NOT called (no truncate mode)
            calculatorMock.verify(() -> MySQLTimeCalculator.timeTruncate(any(MysqlDateTime.class), anyInt()), never());

            assertEquals("Should return TYPE_OK", TypeConversionStatus.TYPE_OK, result);
        }
    }

    /**
     * Test storeInternalWithRound method with MySQL 8.0 and truncate fractional mode
     * Covers the new SessionProperties parameter and the version check logic
     */
    @Test
    public void testStoreInternalWithRoundWithMySQL80AndTruncate() throws Exception {
        try (MockedStatic<InstanceVersion> instanceVersionMock = mockStatic(InstanceVersion.class);
            MockedStatic<MySQLTimeCalculator> calculatorMock = mockStatic(MySQLTimeCalculator.class)) {

            // Mock MySQL 8.0
            instanceVersionMock.when(InstanceVersion::isMYSQL80).thenReturn(true);

            // Mock needToRound to return false to simplify test
            calculatorMock.when(() -> MySQLTimeCalculator.needToRound(anyInt(), anyInt())).thenReturn(false);

            SessionProperties sessionPropsWithTruncate = new SessionProperties(
                ZoneId.systemDefault(),
                CharsetName.defaultCharset(),
                SQLModeFlags.MODE_TIME_TRUNCATE_FRACTIONAL,
                FieldCheckLevel.CHECK_FIELD_WARN
            );

            MysqlDateTime mysqlDateTime = new MysqlDateTime();
            mysqlDateTime.setYear(2023);
            mysqlDateTime.setMonth(7);
            mysqlDateTime.setDay(25);
            mysqlDateTime.setHour(10);
            mysqlDateTime.setMinute(30);
            mysqlDateTime.setSecond(45);
            mysqlDateTime.setSecondPart(123456L);
            mysqlDateTime.setSqlType(Types.TIMESTAMP);

            // Use reflection to access the private storeInternalWithRound method
            Method storeInternalWithRound = DatetimeField.class.getDeclaredMethod(
                "storeInternalWithRound", MysqlDateTime.class, SessionProperties.class, TypeConversionStatus.class);
            storeInternalWithRound.setAccessible(true);

            TypeConversionStatus inputStatus = TypeConversionStatus.TYPE_OK;
            TypeConversionStatus result = (TypeConversionStatus) storeInternalWithRound.invoke(
                datetimeField, mysqlDateTime, sessionPropsWithTruncate, inputStatus);

            // Verify that InstanceVersion.isMYSQL80() was called
            instanceVersionMock.verify(InstanceVersion::isMYSQL80, times(1));

            // Verify that MySQLTimeCalculator.timeTruncate was called
            calculatorMock.verify(() -> MySQLTimeCalculator.timeTruncate(any(MysqlDateTime.class), eq(3)), times(1));

            assertEquals("Should return TYPE_OK", TypeConversionStatus.TYPE_OK, result);
        }
    }

    /**
     * Test storeInternalWithRound method with non-MySQL 8.0
     */
    @Test
    public void testStoreInternalWithRoundWithNonMySQL80() throws Exception {
        try (MockedStatic<InstanceVersion> instanceVersionMock = mockStatic(InstanceVersion.class);
            MockedStatic<MySQLTimeCalculator> calculatorMock = mockStatic(MySQLTimeCalculator.class)) {

            // Mock non-MySQL 8.0
            instanceVersionMock.when(InstanceVersion::isMYSQL80).thenReturn(false);

            // Mock needToRound to return false to simplify test
            calculatorMock.when(() -> MySQLTimeCalculator.needToRound(anyInt(), anyInt())).thenReturn(false);

            SessionProperties sessionPropsWithTruncate = new SessionProperties(
                ZoneId.systemDefault(),
                CharsetName.defaultCharset(),
                SQLModeFlags.MODE_TIME_TRUNCATE_FRACTIONAL,
                FieldCheckLevel.CHECK_FIELD_WARN
            );

            MysqlDateTime mysqlDateTime = new MysqlDateTime();
            mysqlDateTime.setYear(2023);
            mysqlDateTime.setMonth(7);
            mysqlDateTime.setDay(25);
            mysqlDateTime.setHour(10);
            mysqlDateTime.setMinute(30);
            mysqlDateTime.setSecond(45);
            mysqlDateTime.setSecondPart(123456L);
            mysqlDateTime.setSqlType(Types.TIMESTAMP);

            Method storeInternalWithRound = DatetimeField.class.getDeclaredMethod(
                "storeInternalWithRound", MysqlDateTime.class, SessionProperties.class, TypeConversionStatus.class);
            storeInternalWithRound.setAccessible(true);

            TypeConversionStatus inputStatus = TypeConversionStatus.TYPE_OK;
            TypeConversionStatus result = (TypeConversionStatus) storeInternalWithRound.invoke(
                datetimeField, mysqlDateTime, sessionPropsWithTruncate, inputStatus);

            // Verify that InstanceVersion.isMYSQL80() was called
            instanceVersionMock.verify(InstanceVersion::isMYSQL80, times(1));

            // Verify that MySQLTimeCalculator.timeTruncate was NOT called
            calculatorMock.verify(() -> MySQLTimeCalculator.timeTruncate(any(MysqlDateTime.class), anyInt()), never());

            assertEquals("Should return TYPE_OK", TypeConversionStatus.TYPE_OK, result);
        }
    }
}
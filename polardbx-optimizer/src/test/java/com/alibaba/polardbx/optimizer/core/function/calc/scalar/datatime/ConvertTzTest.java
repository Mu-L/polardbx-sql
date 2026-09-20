package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

public class ConvertTzTest {
    RelDataTypeFactory typeFactory =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    @Test
    public void test1() {
        ExecutionContext context = new ExecutionContext();
        Object[] args = new Object[] {
            "2025-03-25 03:16:35",
            "+8:00",
            "+00:00"
        };

        ConvertTz convertTz = new ConvertTz();
        Field field = new Field(typeFactory.createSqlType(SqlTypeName.DATETIME, 3));
        convertTz.setResultField(field);

        OriginalTimestamp ret = (OriginalTimestamp) convertTz.compute(args, context);

        Assert.assertEquals(
            StringTimeParser.parseDatetime("2025-03-24 19:16:35".getBytes()).toPackedLong(),
            ret.getMysqlDateTime().toPackedLong()
        );
    }

    @Test
    public void test2() {
        // mysql> SELECT CONVERT_TZ('2004-01-01 12:00:00','GMT','MET');
        //        -> '2004-01-01 13:00:00'
        // mysql> SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00');
        //        -> '2004-01-01 22:00:00'
        doTest("2004-01-01 12:00:00", "GMT", "MET", "2004-01-01 13:00:00");

        doTest("2004-01-01 12:00:00", "+00:00", "+10:00", "2004-01-01 22:00:00");
    }

    private void doTest(String fromDatetime, String fromTimeZone, String toTimeZone, String expectedDatetime) {
        ExecutionContext context = new ExecutionContext();
        Object[] args = new Object[] {
            fromDatetime,
            fromTimeZone,
            toTimeZone
        };

        ConvertTz convertTz = new ConvertTz();
        Field field = new Field(typeFactory.createSqlType(SqlTypeName.DATETIME, 3));
        convertTz.setResultField(field);

        OriginalTimestamp ret = (OriginalTimestamp) convertTz.compute(args, context);

        Assert.assertEquals(
            StringTimeParser.parseDatetime(expectedDatetime.getBytes()).toPackedLong(),
            ret.getMysqlDateTime().toPackedLong()
        );
    }

}
package com.alibaba.polardbx.optimizer.core.function.calc.scalar.trx;

import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import io.airlift.slice.Slice;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.TYPE_FACTORY;

public class TsoToTimeTest {
    @Test
    public void test() {
        TsoToTime tsoToTime = new TsoToTime();
        SqlTsoToTimeFunction sqlTsoToTimeFunction = new SqlTsoToTimeFunction("TSO_TO");
        tsoToTime.setResultField(new Field(TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, 2000)));
        ExecutionContext context = new ExecutionContext();
        Object ret = tsoToTime.compute(new Object[] {"7299978442710188096"}, context);
        String actual = new String(((Slice) ret).getBytes());
        Assert.assertEquals("2025-02-25 10:28:07.783", actual);

        ret = tsoToTime.compute(new Object[] {"7299978442710188096", "+08:00"}, context);
        actual = new String(((Slice) ret).getBytes());
        Assert.assertEquals("2025-02-25 10:28:07.783", actual);

        ret = tsoToTime.compute(new Object[] {"7299978442710188096", "GMT+06:00 "}, context);
        actual = new String(((Slice) ret).getBytes());
        Assert.assertEquals("2025-02-25 08:28:07.783", actual);

        context.setServerVariables(new HashMap<>());
        context.getServerVariables().put("time_zone", "SYSTEM");
        context.getServerVariables().put("system_time_zone", "CST");
        ret = tsoToTime.compute(new Object[] {"7299978442710188096"}, context);
        actual = new String(((Slice) ret).getBytes());
        Assert.assertEquals("2025-02-25 10:28:07.783", actual);

        ret = tsoToTime.compute(new Object[] {null}, context);
        Assert.assertNull(ret);

        Assert.assertEquals("TSO_TO_TIME", tsoToTime.getFunctionNames()[0]);
        Assert.assertEquals(0, tsoToTime.getScale());
        Assert.assertEquals(0, tsoToTime.getPrecision());
    }
}

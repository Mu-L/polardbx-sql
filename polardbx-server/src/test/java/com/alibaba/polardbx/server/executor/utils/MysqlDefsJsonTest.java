package com.alibaba.polardbx.server.executor.utils;

import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MysqlDefsJsonTest {
    private static final String JSON_VALUE =
        "{\"ascii\":\"value\",\"latin\":\"é\",\"nested\":{\"items\":[1,true,null]}}";

    @Test
    public void testJsonUsesExecuteResultCharset() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString(1)).thenReturn(JSON_VALUE);

        for (String charset : new String[] {"utf8mb4", "gbk", "latin1"}) {
            byte[] fieldValue = MysqlDefs.resultSetToByte(resultSet, 1, MysqlDefs.FIELD_TYPE_JSON,
                false, true, charset);
            byte[] expectedPayload = StringUtil.encode(JSON_VALUE, charset);

            Assert.assertTrue("The fixture must use a one-byte length-encoded integer",
                expectedPayload.length < 251);
            Assert.assertEquals("Unexpected JSON payload length for " + charset,
                expectedPayload.length, fieldValue[0] & 0xff);
            Assert.assertArrayEquals("Unexpected JSON payload encoding for " + charset,
                expectedPayload, Arrays.copyOfRange(fieldValue, 1, fieldValue.length));
        }
    }
}

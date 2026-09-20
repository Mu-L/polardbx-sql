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

package com.alibaba.polardbx.common;

import com.alibaba.polardbx.common.utils.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SQLModeTest {
    private static final Random R = new Random();

    String ANSI = "ANSI";
    String DB_2 = "DB2";
    String MAXDB = "MAXDB";
    String MSSQL = "MSSQL";
    String ORACLE = "ORACLE";
    String POSTGRESQL = "POSTGRESQL";
    String TRADITIONAL = "TRADITIONAL";
    String[] theCombined = {ANSI, DB_2, MAXDB, MSSQL, ORACLE, POSTGRESQL, TRADITIONAL};
    List<String> allSqlMode;

    public SQLModeTest() {
        allSqlMode = Arrays.stream(SQLMode.values()).map(Enum::name).collect(
            Collectors.toList());
        allSqlMode.addAll(Arrays.asList(theCombined));
    }

    public String randomSqlModeStr() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < allSqlMode.size(); i++) {
            if (R.nextInt() % 2 == 0) {
                stringBuilder.append(allSqlMode.get(i));
            }
        }
        return stringBuilder.toString();
    }

    @Test
    public void test() {
        IntStream.range(0, 1 << 10).forEach(
            i -> {
                // convert from sql mode to flag
                String sqlModeStr = randomSqlModeStr();
                long flag = SQLMode.convertToFlag(sqlModeStr);

                // check all sql mode
                SQLMode.convertFromFlag(flag).forEach(
                    sqlMode -> Assert.assertTrue((flag & sqlMode.getModeFlag()) != 0)
                );
            }
        );
    }

    /**
     * 专门测试 MODE_TIME_TRUNCATE_FRACTIONAL 的编码解码功能
     */
    @Test
    public void testTimeTruncateFractionalEncodeDecode() {
        // 测试单独的 TIME_TRUNCATE_FRACTIONAL 模式
        String singleMode = "TIME_TRUNCATE_FRACTIONAL";
        long flag = SQLMode.convertToFlag(singleMode);
        
        // 验证编码结果
        Assert.assertTrue(flag == SQLModeFlags.MODE_TIME_TRUNCATE_FRACTIONAL);

        // 验证解码结果
        Set<SQLMode> decodedModes = SQLMode.convertFromFlag(flag);
        Assert.assertTrue(decodedModes.contains(SQLMode.TIME_TRUNCATE_FRACTIONAL));
        Assert.assertTrue(decodedModes.size() == 1);

        // 测试与其他模式组合
        String combinedMode = "STRICT_TRANS_TABLES,TIME_TRUNCATE_FRACTIONAL,NO_ZERO_DATE";
        long combinedFlag = SQLMode.convertToFlag(combinedMode);

        // 验证组合编码结果包含 TIME_TRUNCATE_FRACTIONAL
        Assert.assertTrue((combinedFlag & SQLModeFlags.MODE_TIME_TRUNCATE_FRACTIONAL) != 0);

        // 验证组合解码结果
        Set<SQLMode> combinedDecodedModes = SQLMode.convertFromFlag(combinedFlag);
        Assert.assertTrue(combinedDecodedModes.contains(SQLMode.TIME_TRUNCATE_FRACTIONAL));
        Assert.assertTrue(combinedDecodedModes.contains(SQLMode.STRICT_TRANS_TABLES));
        Assert.assertTrue(combinedDecodedModes.contains(SQLMode.NO_ZERO_DATE));

        // 测试编码解码的对称性
        long reEncodedFlag = 0L;
        for (SQLMode mode : combinedDecodedModes) {
            reEncodedFlag |= mode.getModeFlag();
        }
        Assert.assertTrue(reEncodedFlag == combinedFlag);

        // 测试边界情况：空字符串
        long emptyFlag = SQLMode.convertToFlag("");
        Assert.assertTrue(emptyFlag == 0L);

        // 测试边界情况：null
        long nullFlag = SQLMode.convertToFlag(null);
        Assert.assertTrue(nullFlag == 0L);

        // 测试包含但不完全匹配的情况
        String partialMatch = "TIME_TRUNCATE_FRACTIONAL_EXTRA";
        long partialFlag = SQLMode.convertToFlag(partialMatch);
        Assert.assertTrue((partialFlag & SQLModeFlags.MODE_TIME_TRUNCATE_FRACTIONAL) != 0);
    }
}
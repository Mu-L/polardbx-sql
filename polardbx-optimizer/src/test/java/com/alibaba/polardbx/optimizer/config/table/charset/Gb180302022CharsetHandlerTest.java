package com.alibaba.polardbx.optimizer.config.table.charset;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.config.table.collation.Gb180302022Unicode520CiCollationHandler;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.optimizer.config.table.collation.Gb180302022Unicode520CiCollationHandler.codeOfGB180302022;
import static org.junit.Assert.*;

public class Gb180302022CharsetHandlerTest {

    @Test
    public void testJavaSupportGb180302022() {
        Charset charset = Charset.forName("GB18030");
        Assert.assertTrue(charset.aliases().contains("gb18030-2022"));
    }

    @Test
    public void testBmp() {
        InputStream inputStream =
            Gb180302022CharsetHandlerTest.class.getResourceAsStream("GB18030_2022_MappingTableBMP.txt");
        testGb180302022(inputStream);
    }

    @Test
    public void testSmp() {
        InputStream inputStream =
            Gb180302022CharsetHandlerTest.class.getResourceAsStream("GB18030_2022_MappingTableSMP.txt");
        testGb180302022(inputStream);
    }

    private void testGb180302022(InputStream testCaseInputStream) {
        Gb180302022CharsetHandler handler = new Gb180302022CharsetHandler(CollationName.GB18030_2022_UNICODE_520_CI);
        Gb180302022Unicode520CiCollationHandler collationHandler =
            (Gb180302022Unicode520CiCollationHandler) handler.getCollationHandler();

        int stringCnt = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(testCaseInputStream))) {
            String line;
            SortKey lastSortKey = null;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\t");
                assertEquals(2, tokens.length);
                String unicodeStr = tokens[0];
                String gb18030Str = tokens[1];
                Pair<Boolean, Long> hexRst = hexStringToULong(unicodeStr.getBytes(), 0, unicodeStr.length());
                if (hexRst.getKey()) {
                    throw new AssertionError("Failed to parse hex string");
                }

                byte[] gb18030 = new byte[4];
                int mb_len = hexStringToByteArray(gb18030, gb18030Str.getBytes(), 0, gb18030Str.length());
                if (mb_len == 0) {
                    throw new AssertionError("Failed to parse hex string");
                }

//                System.out.printf("unicode: %s, gb18030: %s, character: %s\n", unicodeStr, gb18030Str,
//                    new String(gb18030, 0, mb_len, handler.getCharset()));
                Slice slice = Slices.wrappedBuffer(gb18030);
                SortKey curSortKey = collationHandler.getSortKey(slice, 4);
                if (lastSortKey != null) {
                    lastSortKey.compareTo(curSortKey);
                }
                lastSortKey = curSortKey;
                Assert.assertEquals(hexRst.getValue().longValue(), codeOfGB180302022(new BasicSliceInput(slice)));
                stringCnt++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.printf("%d gb18030 characters pass test.\n", stringCnt);
    }

    int hexCharToInt(byte c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        } else if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        } else if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        } else {
            return -1;  // 非法字符
        }
    }

    Pair<Boolean, Long> hexStringToULong(byte[] hexStr, int offset, int len) {
        long result = 0;
        for (int i = offset; i < offset + len; i++) {
            int v = hexCharToInt(hexStr[i]);
            if (v == -1) {
                return Pair.of(true, -1L);
            }
            result = result << 4 | v;
        }
        return Pair.of(false, result);
    }

    int hexStringToByteArray(byte[] byteArray, byte[] hexStr, int pos, int len) {
        int j = 0;
        for (int i = pos; i < pos + len / 2; i++) {
            int highNibble = hexCharToInt(hexStr[i * 2]);
            int lowNibble = hexCharToInt(hexStr[i * 2 + 1]);
            if (highNibble == -1 || lowNibble == -1) {
                return 0;
            }
            byteArray[j++] = (byte) ((((byte) (highNibble & 0xff)) << 4) | ((byte) (lowNibble & 0xff)));
        }
        return len / 2;
    }
}
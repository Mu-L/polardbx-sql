package com.alibaba.polardbx.server.encdb.mask;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskType;
import junit.framework.TestCase;
import org.junit.Test;

import java.sql.Timestamp;

public class EncdbMaskUtilTest extends TestCase {

    @Test
    public void testAllMask() {
        assertEquals(EncdbMaskUtil.allMask("abc"), "***");
        assertEquals(EncdbMaskUtil.allMask(""), "");
        assertEquals(EncdbMaskUtil.allMask("abcdefg"), "*******");
    }

    @Test
    public void testFixPosMask() {
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {1, 3}), "***defg");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {4, 6}), "abc***g");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {7, 12}), "abcdef*");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {1, 9}), "*******");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {1, 7}), "*******");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {0, 1}), "*bcdefg");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {-3, -2}), "abcd**g");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {-10, -2}), "******g");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {-10, -9}), "abcdefg");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {1, 2, 4, 4}), "**c*efg");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {1, 2, -2, -1}), "**cde**");
        assertEquals(EncdbMaskUtil.fixPosMask("abcdefg", new int[] {1, 2, 4, 4, -3, -2}), "**c***g");
    }

    @Test
    public void testFixCharMask() {
        assertEquals(EncdbMaskUtil.fixCharMask("abcdefg", "bc"), "a**defg");
        assertEquals(EncdbMaskUtil.fixCharMask("abcdefg", "cd"), "ab**efg");
        assertEquals(EncdbMaskUtil.fixCharMask("abcdefg", "de"), "abc**fg");
        assertEquals(EncdbMaskUtil.fixCharMask("abcdefg", "h"), "abcdefg");
    }

    @Test
    public void testMapReplace() {
        assertEquals(EncdbMaskUtil.mapReplace("abcdefg", "b", "h"), "ahcdefg");
        assertEquals(EncdbMaskUtil.mapReplace("abcdefg", "cd", "h"), "abhefg");
        assertEquals(EncdbMaskUtil.mapReplace("abcdefg", "df", "he"), "abcdefg");
        assertEquals(EncdbMaskUtil.mapReplace("abcdefg", "e", ""), "abcdfg");
        assertEquals(EncdbMaskUtil.mapReplace("abcdefg", "", "d"), "dadbdcdddedfdgd");
    }

    @Test
    public void testRandomReplace() {
        for (int i = 0; i < 100; i++) {
            assertEquals(EncdbMaskUtil.randomReplace("abcdefg").length(), "abcdefg".length());
            assertNotSame(EncdbMaskUtil.randomReplace("abcdefg"), "abcdefg");
        }
    }

    @Test
    public void testNumberRounding() {
        assertEquals(EncdbMaskUtil.numberRounding(1.2345), 1);
        assertEquals(EncdbMaskUtil.numberRounding(2.23146), 2);
        assertEquals(EncdbMaskUtil.numberRounding(-2.23146), -2);
        assertEquals(EncdbMaskUtil.numberRounding(12.23146f), 12);
        assertEquals(EncdbMaskUtil.numberRounding(12.0d), 12);
    }

    @Test
    public void testDateRounding() {
        assertEquals(
            EncdbMaskUtil.dateRounding(Timestamp.valueOf("2020-12-12 12:12:12"), EncdbMaskType.DateRoundingLevel.DAY),
            Timestamp.valueOf("2020-12-12 00:00:00"));
        assertEquals(
            EncdbMaskUtil.dateRounding(Timestamp.valueOf("2020-12-12 12:12:12"), EncdbMaskType.DateRoundingLevel.MONTH),
            Timestamp.valueOf("2020-12-01 00:00:00"));
        assertEquals(
            EncdbMaskUtil.dateRounding(Timestamp.valueOf("2020-12-12 12:12:12"), EncdbMaskType.DateRoundingLevel.YEAR),
            Timestamp.valueOf("2020-01-01 00:00:00"));
        assertEquals(
            EncdbMaskUtil.dateRounding(Timestamp.valueOf("2020-12-12 12:12:12"), EncdbMaskType.DateRoundingLevel.HOUR),
            Timestamp.valueOf("2020-12-12 12:00:00"));
        assertEquals(EncdbMaskUtil.dateRounding(Timestamp.valueOf("2020-12-12 12:12:12"),
            EncdbMaskType.DateRoundingLevel.MINUTE), Timestamp.valueOf("2020-12-12 12:12:00"));
        assertEquals(EncdbMaskUtil.dateRounding(Timestamp.valueOf("2020-12-12 12:12:12"),
            EncdbMaskType.DateRoundingLevel.SECOND), Timestamp.valueOf("2020-12-12 12:12:12"));
    }

    @Test
    public void testLeftShiftString() {
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 0), "abcdefg");
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 1), "bcdefg");
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 2), "cdefg");
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 3), "defg");
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 4), "efg");
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 5), "fg");
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 6), "g");
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 7), "");
        assertEquals(EncdbMaskUtil.leftShiftString("abcdefg", 8), "");
    }

    @Test
    public void testNumberReplace() {
        assertEquals(EncdbMaskUtil.numberReplace("d5a123s4o", 0), "d0a000s0o");
        assertEquals(EncdbMaskUtil.numberReplace("d5a123s4o", 1), "d1a111s1o");
        assertEquals(EncdbMaskUtil.numberReplace("d5a123s4o", 12), "d2a222s2o");
        assertEquals(EncdbMaskUtil.numberReplace("d5a123s4o", -12), "d2a222s2o");
    }

    @Test
    public void testNumberReplace2() {
        for (int i = 0; i < 1000; i++) {
            int rand = Integer.parseInt(EncdbMaskUtil.numberReplace("1234", 10, 500));
            Assert.assertTrue(rand >= 10);
            Assert.assertTrue(rand <= 500);

        }

        System.out.println("-----");
        assertEquals(EncdbMaskUtil.emailPersonMask("zhangsan@corp.com"), "********@corp.com");
        assertEquals(EncdbMaskUtil.emailPersonMask("zhangsan@corp1.com"), "********@corp1.com");
        assertEquals(EncdbMaskUtil.emailPersonMask("zhangsan@corp12das.com"), "********@corp12das.com");
        assertEquals(EncdbMaskUtil.emailPersonMask("zhang@corp12das.com"), "*****@corp12das.com");

        assertEquals(EncdbMaskUtil.emailPersonAndCompanyMask("zhangsan@corp.com"), "********@****.com");
        assertEquals(EncdbMaskUtil.emailPersonAndCompanyMask("zhangsan@corp1.com"), "********@*****.com");
        assertEquals(EncdbMaskUtil.emailPersonAndCompanyMask("zhangsan@corp12das.com"), "********@*********.com");
        assertEquals(EncdbMaskUtil.emailPersonAndCompanyMask("zhang@corp12das.com"), "*****@*********.com");

        System.out.println("dsadsas");
    }

    @Test
    public void emailPersonMaskTest() {
        assertEquals(EncdbMaskUtil.emailPersonMask("zhangsan@corp.com"), "********@corp.com");
        assertEquals(EncdbMaskUtil.emailPersonMask("zhangsan@corp1.com"), "********@corp1.com");
        assertEquals(EncdbMaskUtil.emailPersonMask("zhangsan@corp12das.com"), "********@corp12das.com");
        assertEquals(EncdbMaskUtil.emailPersonMask("zhang@corp12das.com"), "*****@corp12das.com");
    }

    @Test
    public void emailPersonAndCompanyMaskTest() {

    }

}
package com.alibaba.polardbx.server.encdb;

import junit.framework.TestCase;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertEquals;

public class EncdbUtilsTest extends TestCase {

    public void testCompareEncjdbcVersion() {
        // 测试版本相等的情况
        assertEquals(0, EncdbUtils.compareEncjdbcVersion("1.2.22", "1.2.22"));

        // 测试第一个版本大于第二个版本的情况
        assertEquals(1, EncdbUtils.compareEncjdbcVersion("1.2.23", "1.2.22"));
        assertEquals(1, EncdbUtils.compareEncjdbcVersion("1.3.22", "1.2.22"));
        assertEquals(1, EncdbUtils.compareEncjdbcVersion("2.2.22", "1.2.22"));
        assertEquals(1, EncdbUtils.compareEncjdbcVersion("2.2.12", "1.2.2"));

        // 测试第一个版本小于第二个版本的情况
        assertEquals(-1, EncdbUtils.compareEncjdbcVersion("1.2.21", "1.2.22"));
        assertEquals(-1, EncdbUtils.compareEncjdbcVersion("1.1.22", "1.2.22"));
        assertEquals(-1, EncdbUtils.compareEncjdbcVersion("0.2.22", "1.2.22"));

        // 测试带有-SNAPSHOT后缀的版本比较
        assertEquals(0, EncdbUtils.compareEncjdbcVersion("1.2.22-SNAPSHOT", "1.2.22"));
        assertEquals(0, EncdbUtils.compareEncjdbcVersion("1.2.22", "1.2.22-SNAPSHOT"));
        assertEquals(0, EncdbUtils.compareEncjdbcVersion("1.2.22-SNAPSHOT", "1.2.22-SNAPSHOT"));

        // 测试长度不一致的情况
        assertEquals(-1, EncdbUtils.compareEncjdbcVersion("1.2", "1.2.22"));
        assertEquals(-1, EncdbUtils.compareEncjdbcVersion("1", "1.2.22"));
    }

}
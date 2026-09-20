package com.alibaba.polardbx.optimizer.ttl;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;

public class NormalizedTimeSequenceGenerator2Test {

    @Test
    public void testNormalizeDateTimeFor1Year() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "year";         // 时间间隔单位
        int intervalValue = 1;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-01-01T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-12-31T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("2025-01-01T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }

    @Test
    public void testNormalizeDateTimeFor3Year() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "year";         // 时间间隔单位
        int intervalValue = 3;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1973-01-01T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1975-12-31T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1973-01-01T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }

    @Test
    public void testNormalizeDateTimeFor1Month() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "month";         // 时间间隔单位
        int intervalValue = 1;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1973-01-03T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1973-01-06T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1973-01-01T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }

    @Test
    public void testNormalizeDateTimeFor3Month() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "month";         // 时间间隔单位
        int intervalValue = 3;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1973-04-03T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1973-06-30T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1973-04-01T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }

    @Test
    public void testNormalizeDateTimeFor12Month() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "month";         // 时间间隔单位
        int intervalValue = 12;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1973-04-03T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1973-06-30T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1973-01-01T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }


    @Test
    public void testNormalizeDateTimeFor1Week() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "week";         // 时间间隔单位
        int intervalValue = 1;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1973-01-01T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1973-01-02T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1972-12-31T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }


    @Test
    public void testNormalizeDateTimeFor3Week() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "week";         // 时间间隔单位
        int intervalValue = 3;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1973-02-10T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1973-02-20T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);



        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1973-02-04T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }

    @Test
    public void testNormalizeDateTimeFor1Day() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "day";         // 时间间隔单位
        int intervalValue = 1;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1973-02-10T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1973-02-10T04:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1973-02-10T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }

    @Test
    public void testNormalizeDateTimeFor7Day() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "day";         // 时间间隔单位
        int intervalValue = 7;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1970-01-08T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1970-01-14T04:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1970-01-08T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }


    @Test
    public void testNormalizeDateTimeFor13Day() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "day";         // 时间间隔单位
        int intervalValue = 13;                  // 时间间隔值（1 个月）
        String currDatetime1 = "1970-01-15T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "1970-01-26T04:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）

        // 调用函数
        String result1 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime1);

        // 调用函数
        String result2 = NormalizedTimeSequenceGenerator.normalizeDatetimeByIntervalAndUnit(baseTime, intervalUnit, intervalValue, currDatetime2);

        System.out.println(String.format("currDatetime1=%s, result1=%s", currDatetime1, result1));
        System.out.println(String.format("currDatetime2=%s, result2=%s", currDatetime2, result2));
        Assert.assertTrue(result1.equals("1970-01-14T00:00"));
        Assert.assertTrue(result1.equals(result2));
    }
    
}
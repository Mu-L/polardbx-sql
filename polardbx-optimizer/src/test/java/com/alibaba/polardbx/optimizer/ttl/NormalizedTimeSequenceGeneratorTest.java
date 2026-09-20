package com.alibaba.polardbx.optimizer.ttl;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;

public class NormalizedTimeSequenceGeneratorTest {

    @Test
    public void testGenerateTimeSeq1Year() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "year";         // 时间间隔单位
        int intervalValue = 1;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-01-01T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-12-31T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 3;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }

    @Test
    public void testGenerateTimeSeq3Year() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "year";         // 时间间隔单位
        int intervalValue = 12;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-07-25T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 30;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }

    @Test
    public void testGenerateTimeSeq1Month() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "month";         // 时间间隔单位
        int intervalValue = 1;                  // 时间间隔值（1 个月）
        String currDatetime = "2023-10-31T00:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 10;
        // 调用函数
        List<LocalDateTime> result = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(
            baseTime,
            intervalUnit,
            intervalValue,
            currDatetime,
            timePointCount);

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间值序列：");
        for (LocalDateTime time : result) {
            System.out.println(time);
        }
    }


    @Test
    public void testGenerateTimeSeq3Month() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "month";         // 时间间隔单位
        int intervalValue = 3;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-07-25T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 30;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }


    @Test
    public void testGenerateTimeSeq12Month() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "month";         // 时间间隔单位
        int intervalValue = 12;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-07-25T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 30;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }


    @Test
    public void testGenerateTimeSeq7Days() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "day";         // 时间间隔单位
        int intervalValue = 7;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-07-20T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 5;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }


    @Test
    public void testGenerateTimeSeq30Days() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "day";         // 时间间隔单位
        int intervalValue = 30;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-07-20T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 5;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }



    @Test
    public void testGenerateTimeSeq5Days() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "day";         // 时间间隔单位
        int intervalValue = 20;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-07-25T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 5;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }

    @Test
    public void testGenerateTimeSeq1Weeks() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "week";         // 时间间隔单位
        int intervalValue = 1;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-07-24T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 5;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }

    @Test
    public void testGenerateTimeSeq3Weeks_case1() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "week";         // 时间间隔单位
        int intervalValue = 3;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-07-24T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 5;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

        Assert.assertTrue(result1.size() == result2.size());
        for (int i = 0; i < result1.size(); i++) {
            LocalDateTime ts1 = result1.get(i);
            LocalDateTime ts2 = result2.get(i);
            Assert.assertTrue(ts1.equals(ts2));
        }
    }

    @Test
    public void testGenerateTimeSeq3Weeks_case2() {
        // 定义参数
        String baseTime = "1970-01-01T00:00:00"; // 基准时间, UTC
        String intervalUnit = "week";         // 时间间隔单位
        int intervalValue = 3;                  // 时间间隔值（1 个月）
        String currDatetime1 = "2025-07-23T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        String currDatetime2 = "2025-08-04T02:00:00"; // 当前时间戳（2023-10-31 00:00:00 UTC）
        int timePointCount = 5;

        // 调用函数
        List<LocalDateTime> result1 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime1, timePointCount);

        // 调用函数
        List<LocalDateTime> result2 = NormalizedTimeSequenceGenerator.generateNormalizedTimeSequence(baseTime, intervalUnit, intervalValue, currDatetime2, timePointCount);


        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result1) {
            System.out.println(ts.toString());
        }

        // 打印结果
        System.out.println("比当前时间大的最接近的 10 个时间戳：");
        for (LocalDateTime ts : result2) {
            System.out.println(ts.toString());
        }

//        Assert.assertTrue(result1.size() == result2.size());
//        for (int i = 0; i < result1.size(); i++) {
//            LocalDateTime ts1 = result1.get(i);
//            LocalDateTime ts2 = result2.get(i);
//            Assert.assertTrue(ts1.equals(ts2));
//        }
    }
}
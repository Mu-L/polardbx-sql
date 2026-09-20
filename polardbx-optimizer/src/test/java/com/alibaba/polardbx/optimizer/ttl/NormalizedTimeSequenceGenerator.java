package com.alibaba.polardbx.optimizer.ttl;

import org.jetbrains.annotations.NotNull;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;

public class NormalizedTimeSequenceGenerator {

    public static List<LocalDateTime> generateNormalizedTimeSequence(
        String baseTimeStr,
        String intervalUnit,
        int intervalValue,
        String currentDateTimeStr,
        int latestTimePointCount) {

        // define base pivot time
        LocalDateTime baseDateTime = LocalDateTime.parse(baseTimeStr);

        // fetch current time
        LocalDateTime currentDateTime = LocalDateTime.parse(currentDateTimeStr);

        // init result list
        List<LocalDateTime> resultTimes = new ArrayList<>();

        // gen datetime sequence by the  base pivot time
        LocalDateTime currentGeneratedTime = baseDateTime;
        while (resultTimes.size() < latestTimePointCount) {
            // normalize curr new generated datetime
            currentGeneratedTime = normalizeDatetimeValueByIntervalUnit(intervalUnit, currentGeneratedTime);

            // increment the currentGeneratedTime by timeInterval and timeUnit
            currentGeneratedTime = computeNewDatetimeByAddInterval(intervalUnit, intervalValue, currentGeneratedTime);

            // 如果生成的时间大于当前时间，则加入结果列表
            if (currentGeneratedTime.isAfter(currentDateTime)) {
                resultTimes.add(currentGeneratedTime);
            }
        }

        return resultTimes;
    }

    protected static class NormalizedTimeSequenceResult {
        protected List<LocalDateTime> targetTimeSequenceList;
        protected String beginDatetimeStr;
        protected String endDatetimeStr;
        protected String pivotPointDatetimeStr;
        protected List<LocalDateTime> timeSequenceListAfterPivotPointStr;
        protected List<LocalDateTime> timeSequenceListBeforePivotPointStr;

        public NormalizedTimeSequenceResult() {
        }
    }

    public static String normalizeDatetimeByIntervalAndUnit(
        String baseTimeStr,
        String intervalUnit,
        int intervalValue,
        String pivotPointDatetimeStr) {

        // define base pivot time
        LocalDateTime baseDateTime = LocalDateTime.parse(baseTimeStr);

        // fetch current time
        LocalDateTime pivotDateTime = LocalDateTime.parse(pivotPointDatetimeStr);

        // timePoint after pivotPoint
        List<LocalDateTime> dateTimePointsAfterPivotPoint = new ArrayList<>();

        // gen datetime sequence by the  base pivot time
        LocalDateTime currentGeneratedTime = baseDateTime;
        LocalDateTime lastGeneratedTimeAfterNormization = null;
        while (dateTimePointsAfterPivotPoint.isEmpty()) {

            // normalize curr new generated datetime
            currentGeneratedTime = normalizeDatetimeValueByIntervalUnit(intervalUnit, currentGeneratedTime);

            // increment the currentGeneratedTime by timeInterval and timeUnit
            lastGeneratedTimeAfterNormization = currentGeneratedTime;
            currentGeneratedTime = computeNewDatetimeByAddInterval(intervalUnit, intervalValue, currentGeneratedTime);

            if (currentGeneratedTime.isAfter(pivotDateTime)) {
                dateTimePointsAfterPivotPoint.add(currentGeneratedTime);
                break;
            }
        }

        return lastGeneratedTimeAfterNormization.toString();
    }

    private static LocalDateTime computeNewDatetimeByAddInterval(String intervalUnit, int intervalValue,
                                                                 LocalDateTime currentGeneratedTime) {
        switch (intervalUnit.toLowerCase()) {
        case "day":
            currentGeneratedTime = currentGeneratedTime.plus(intervalValue, ChronoUnit.DAYS);
            break;
        case "week":
            currentGeneratedTime = currentGeneratedTime.plus(intervalValue, ChronoUnit.WEEKS);
            break;
        case "month":
            currentGeneratedTime = currentGeneratedTime.plus(intervalValue, ChronoUnit.MONTHS);
            break;
        case "year":
            currentGeneratedTime = currentGeneratedTime.plus(intervalValue, ChronoUnit.YEARS);
            break;
        }
        return currentGeneratedTime;
    }

    private static @NotNull LocalDateTime normalizeDatetimeValueByIntervalUnit(String intervalUnit,
                                                                               LocalDateTime currentGeneratedTime) {
        switch (intervalUnit.toLowerCase()) {
        case "day":
            currentGeneratedTime = normalizeToDay(currentGeneratedTime);
            break;
        case "week":
            currentGeneratedTime = normalizeToWeek(currentGeneratedTime);
            break;
        case "month":
            currentGeneratedTime = normalizeToMonth(currentGeneratedTime);
            break;
        case "year":
            currentGeneratedTime = normalizeToYear(currentGeneratedTime);
            break;
        default:
            throw new IllegalArgumentException("Unsupported interval unit. Use 'day', 'week', 'month' or 'year'.");
        }
        return currentGeneratedTime;
    }

    private static LocalDateTime normalizeToDay(LocalDateTime dateTime) {
        // Normalize to the 01 00:00:00 of one day
        return dateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
    }

    private static LocalDateTime normalizeToWeek(LocalDateTime dateTime) {
        // Normalize to the first day sunday of one week
        return dateTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY))
            .withHour(0).withMinute(0).withSecond(0).withNano(0);
    }

    private static LocalDateTime normalizeToMonth(LocalDateTime dateTime) {
        // Normalize to the 01 00:00:00 of one year
        return dateTime.withDayOfMonth(1)
            .withHour(0).withMinute(0).withSecond(0).withNano(0);
    }

    private static LocalDateTime normalizeToYear(LocalDateTime dateTime) {
        // Normalize to the 01-01 00:00:00 of one year
        return dateTime.withMonth(1).withDayOfMonth(1)
            .withHour(0).withMinute(0).withSecond(0).withNano(0);
    }
}
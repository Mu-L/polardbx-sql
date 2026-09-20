package com.alibaba.polardbx.optimizer.ttl;

import org.jetbrains.annotations.NotNull;

import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TtlDatetimeNormalizer {

    public static String TTL_NORMALIZATION_BASE_DATETIME = "1970-01-01 00:00:00";
    public static final DateTimeFormatter TTL_ISO_DATETIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat TTL_SIMPLE_DATE_FORMATE = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final ZoneId TTL_DEFAULT_TIME_ZONE_ID = ZoneId.of("+08:00");

    public static String normalizeDatetimeByIntervalAndUnit(
        String baseTimeStr,
        String intervalUnit,
        int intervalValue,
        String pivotPointDatetimeStr) {

        // define base pivot time
        LocalDateTime baseDateTime = LocalDateTime.parse(baseTimeStr, TTL_ISO_DATETIME_FORMATTER);

        // fetch current time
        LocalDateTime pivotDateTime = LocalDateTime.parse(pivotPointDatetimeStr, TTL_ISO_DATETIME_FORMATTER);

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

        String isoStringWithZone = lastGeneratedTimeAfterNormization.format(TTL_ISO_DATETIME_FORMATTER);
        return isoStringWithZone;
    }

    private static LocalDateTime computeNewDatetimeByAddInterval(String intervalUnit,
                                                                 int intervalValue,
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

    /**
     * If dateVal is null, then auto use current time.
     */
    public static String isoFormatDateObj(Date dateVal) {
        try {
            if (dateVal == null) {
                dateVal = new Date();
            }
            String dateTimeString = TTL_SIMPLE_DATE_FORMATE.format(dateVal);
            return dateTimeString;
        } catch (Throwable ex) {
            return null;
        }
    }

    /**
     * Fetch current datetime with target time zone
     */
    public static String fetchCurrentDateTimeWithTimeZone(ZoneId targetTimeZoneId) {
        try {
            ZoneId targetZone = targetTimeZoneId;
            ZonedDateTime zonedDateTime = ZonedDateTime.now(targetZone);
            DateTimeFormatter formatter = TtlDatetimeNormalizer.TTL_ISO_DATETIME_FORMATTER;
            String formattedTime = zonedDateTime.format(formatter);
            return formattedTime;
        } catch (Throwable ex) {
            return null;
        }
    }

    public static Long datetimeStrToUnixTime(String dateTimeStr, ZoneId tzId) {
        try {
            DateTimeFormatter formater = TtlDatetimeNormalizer.TTL_ISO_DATETIME_FORMATTER;
            LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, formater);
            if (tzId == null) {
                tzId = TtlDatetimeNormalizer.TTL_DEFAULT_TIME_ZONE_ID;
            }
            long unixTime = dateTime.atZone(tzId).toEpochSecond();
            return unixTime;

        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
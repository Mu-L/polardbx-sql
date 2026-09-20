package com.alibaba.polardbx.common.utils.time.core;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

import static com.alibaba.polardbx.common.utils.LongUtil.fastGetSmallLongBytesForDate;

/**
 * Mysql datetime is the explicit representation of temporal value. SEE:include/mysql_time.h st_mysql_time
 */
public class MysqlDateTime implements Serializable, MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MysqlDateTime.class).instanceSize();
    /**
     * From year to second
     */
    private long year;
    private long month;
    private long day;
    private long hour;
    private long minute;
    private long second;

    /**
     * It's different from st_mysql_time， where secondPart is nano seconds rather than micro seconds
     */
    private long secondPart;

    /**
     * The jdbc standard representation of type. Types.TIME / Types.TIMESTAMP / Types.DATE/ MySQLTimeTypeUtil.DATETIME_SQL_TYPE
     */
    private int sqlType;

    /**
     * The sign of temporal values.
     */
    private boolean isNeg;

    /**
     * The timezone information. For UTC time calculation.
     */
    @FieldMemoryCounter(value = false)
    private TimeZone timezone;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE;
    }

    public MysqlDateTime() {
        this(0, 0, 0, 0, 0, 0, 0);
    }

    public MysqlDateTime(long year, long month, long day, long hour, long minute, long second, long secondPart) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.secondPart = secondPart;
        this.isNeg = false;
        this.timezone = null;
    }

    public static MysqlDateTime zeroDateTime() {
        return new MysqlDateTime();
    }

    @Override
    public MysqlDateTime clone() {
        MysqlDateTime t = new MysqlDateTime();
        t.year = this.year;
        t.month = this.month;
        t.day = this.day;
        t.hour = this.hour;
        t.minute = this.minute;
        t.second = this.second;
        t.secondPart = this.secondPart;
        t.isNeg = this.isNeg;
        t.timezone = this.timezone;
        t.sqlType = this.sqlType;
        return t;
    }

    /**
     * Get epoch millis from temporal value and ignore the nano seconds
     */
    public long toEpochMillisBySqlType() {
        switch (sqlType) {
        case Types.DATE:
            return toEpochMillsForDate();
        case Types.TIME:
            return toEpochMillsForTime();
        case Types.TIMESTAMP:
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
        default:
            return toEpochMillsForDatetime();
        }
    }

    /**
     * Get epoch millis from temporal value and ignore the nano seconds
     */
    public long toEpochMillsForDatetime() {
        Calendar calendar = MySQLTimeTypeUtil.getCalendar();
        calendar.setTimeZone(InternalTimeZone.DEFAULT_TIME_ZONE);
        calendar.set((int) year, (int) (month - 1), (int) day, (int) hour, (int) minute, (int) second);
        calendar.set(Calendar.MILLISECOND, (int) (secondPart / 1000_000L));
        long millis = calendar.getTimeInMillis();
        return millis;
    }

    /**
     * Get epoch millis from temporal value and ignore the nano seconds
     */
    public long toEpochMillsForDate() {
        Calendar calendar = MySQLTimeTypeUtil.getCalendar();
        calendar.setTimeZone(InternalTimeZone.DEFAULT_TIME_ZONE);
        calendar.set((int) year, (int) (month - 1), (int) day, 0, 0, 0);
        long millis = calendar.getTimeInMillis();
        return millis;
    }

    /**
     * Get epoch millis from temporal value, don't ignore the nano seconds
     */
    public long toEpochMillsForTime() {
        Calendar calendar = MySQLTimeTypeUtil.getCalendar();
        calendar.setTimeZone(InternalTimeZone.DEFAULT_TIME_ZONE);
        if (!isNeg) {
            calendar.set(1970, Calendar.JANUARY, 1, (int) hour, (int) minute, (int) second);
            calendar.set(Calendar.MILLISECOND, (int) (secondPart / 1000_000L));
        } else {
            calendar.set(1970, Calendar.JANUARY, 1, -((int) hour), -((int) minute), -((int) second));
            calendar.set(Calendar.MILLISECOND, -(int) (secondPart / 1000_000L));
        }
        long millis = calendar.getTimeInMillis();
        return millis;
    }

    /**
     * The default representation of datetime using unlimited scale.
     */
    @Override
    public String toString() {
        return toDatetimeString(-1);
    }

    /**
     * Get String representation of the mysql datetime, according to the sql type value.
     */
    public String toStringBySqlType() {
        switch (this.sqlType) {
        case Types.TIMESTAMP:
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
            return toDatetimeString(secondPart == 0 ? 0 : MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);
        case Types.DATE:
            return toDateString();
        case Types.TIME:
            return toTimeString(secondPart == 0 ? 0 : MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);
        default:
            return toDatetimeString(-1);
        }
    }

    /**
     * to HH:MM:SS.[ffffff] according to scale
     */
    public String toTimeString(int scale) {
        String hourString;
        String minuteString;
        String secondString;
        String nanosecondString;
        String zeros = "000000000";

        StringBuffer timestampBuf;

        if (hour < 10) {
            hourString = "0" + hour;
        } else {
            hourString = Long.toString(hour);
        }
        if (minute < 10) {
            minuteString = "0" + minute;
        } else {
            minuteString = Long.toString(minute);
        }
        if (second < 10) {
            secondString = "0" + second;
        } else {
            secondString = Long.toString(second);
        }
        if (secondPart == 0) {
            nanosecondString = "";
        } else {
            nanosecondString = Long.toString(secondPart);
            if (nanosecondString.length() <= 9) {
                // Add leading zeros
                nanosecondString = zeros.substring(0, (9 - nanosecondString.length())) +
                    nanosecondString;
            }

            // Truncate trailing zeros
            char[] nanosChar = new char[nanosecondString.length()];
            nanosecondString.getChars(0, nanosecondString.length(), nanosChar, 0);
            int truncIndex = 8;
            while (nanosChar[truncIndex] == '0') {
                truncIndex--;
            }

            nanosecondString = new String(nanosChar, 0, truncIndex + 1);
        }

        // do a string buffer here instead.
        timestampBuf = new StringBuffer(MySQLTimeTypeUtil.MAX_TIME_WIDTH + nanosecondString.length());
        if (isNeg) {
            timestampBuf.append('-');
        }

        timestampBuf.append(hourString);
        timestampBuf.append(":");
        timestampBuf.append(minuteString);
        timestampBuf.append(":");
        timestampBuf.append(secondString);

        if (!nanosecondString.isEmpty()) {
            //  for nanosecond != 0
            int nanoLen = nanosecondString.length();
            timestampBuf.append(".");
            timestampBuf.append(nanosecondString);

            // append '0'
            if (scale > nanoLen) {
                for (int i = nanoLen; i < scale; i++) {
                    timestampBuf.append('0');
                }
            }
        } else if (scale > 0) {
            timestampBuf.append(".");
            // for nanosecond = 0 but scale > 0
            for (int i = 0; i < scale; i++) {
                timestampBuf.append('0');
            }
        }

        return timestampBuf.toString();
    }

    /**
     * to YYYY-MM-DD format
     */
    public String toDateString() {
        String yearString;
        String monthString;
        String dayString;
        String yearZeros = "0000";
        StringBuffer timestampBuf;

        if (year < 1000) {
            // Add leading zeros
            yearString = "" + year;
            yearString = yearZeros.substring(0, (4 - yearString.length())) +
                yearString;
        } else {
            yearString = "" + year;
        }
        if (month < 10) {
            monthString = "0" + month;
        } else {
            monthString = Long.toString(month);
        }
        if (day < 10) {
            dayString = "0" + day;
        } else {
            dayString = Long.toString(day);
        }

        // do a string buffer here instead.
        timestampBuf = new StringBuffer(MySQLTimeTypeUtil.MAX_DATE_WIDTH);
        if (isNeg) {
            timestampBuf.append('-');
        }
        timestampBuf.append(yearString);
        timestampBuf.append("-");
        timestampBuf.append(monthString);
        timestampBuf.append("-");
        timestampBuf.append(dayString);

        return timestampBuf.toString();
    }

    /**
     * print datetime by scale in format of YYYY-MM-DD hh:mm:ss[.ffffff]
     *
     * @param scale scale = -1 means any scale.
     * @return printed string
     */
    public String toDatetimeString(int scale) {
        String yearString;
        String monthString;
        String dayString;
        String hourString;
        String minuteString;
        String secondString;
        String nanosecondString;
        String zeros = "000000000";
        String yearZeros = "0000";
        StringBuilder timestampBuf;

        if (year < 1000) {
            // Add leading zeros
            yearString = "" + year;
            yearString = yearZeros.substring(0, (4 - yearString.length())) +
                yearString;
        } else {
            yearString = "" + year;
        }
        if (month < 10) {
            monthString = "0" + month;
        } else {
            monthString = Long.toString(month);
        }
        if (day < 10) {
            dayString = "0" + day;
        } else {
            dayString = Long.toString(day);
        }
        if (hour < 10) {
            hourString = "0" + hour;
        } else {
            hourString = Long.toString(hour);
        }
        if (minute < 10) {
            minuteString = "0" + minute;
        } else {
            minuteString = Long.toString(minute);
        }
        if (second < 10) {
            secondString = "0" + second;
        } else {
            secondString = Long.toString(second);
        }
        if (secondPart == 0) {
            nanosecondString = "";
        } else {
            nanosecondString = Long.toString(secondPart);
            if (nanosecondString.length() <= 9) {
                // Add leading zeros
                nanosecondString = zeros.substring(0, (9 - nanosecondString.length())) +
                    nanosecondString;
            }

            // Truncate trailing zeros
            char[] nanosChar = new char[nanosecondString.length()];
            nanosecondString.getChars(0, nanosecondString.length(), nanosChar, 0);
            int truncIndex = 8;
            while (nanosChar[truncIndex] == '0') {
                truncIndex--;
            }

            nanosecondString = new String(nanosChar, 0, truncIndex + 1);
        }

        // do a string buffer here instead.
        timestampBuf = new StringBuilder(20 + nanosecondString.length());
        if (isNeg) {
            timestampBuf.append('-');
        }
        timestampBuf.append(yearString);
        timestampBuf.append("-");
        timestampBuf.append(monthString);
        timestampBuf.append("-");
        timestampBuf.append(dayString);
        timestampBuf.append(" ");
        timestampBuf.append(hourString);
        timestampBuf.append(":");
        timestampBuf.append(minuteString);
        timestampBuf.append(":");
        timestampBuf.append(secondString);

        if (!nanosecondString.isEmpty()) {
            //  for nanosecond != 0
            int nanoLen = nanosecondString.length();
            timestampBuf.append(".");
            timestampBuf.append(nanosecondString);

            // append '0'
            if (scale > nanoLen) {
                for (int i = nanoLen; i < scale; i++) {
                    timestampBuf.append('0');
                }
            }
        } else if (scale > 0) {
            timestampBuf.append(".");
            // for nanosecond = 0 but scale > 0
            for (int i = 0; i < scale; i++) {
                timestampBuf.append('0');
            }
        }

        return timestampBuf.toString();
    }

    /**
     * to YYYY-MM-DD format
     */
    public byte[] fastToDateBytes() throws IOException {
        ByteArrayOutputStream timestampByteBuf = new ByteArrayOutputStream(MySQLTimeTypeUtil.MAX_DATE_WIDTH);
        byte[] yearBytes = fastGetSmallLongBytesForDate(year, 4);
        byte[] monthBytes = fastGetSmallLongBytesForDate(month, 2);
        byte[] dayBytes = fastGetSmallLongBytesForDate(day, 2);
        if (isNeg) {
            timestampByteBuf.write('-');
        }
        timestampByteBuf.write(yearBytes);
        timestampByteBuf.write('-');
        timestampByteBuf.write(monthBytes);
        timestampByteBuf.write('-');
        timestampByteBuf.write(dayBytes);

        return timestampByteBuf.toByteArray();
    }

    public byte[] fastToDatetimeBytes(int scale) throws IOException {
        ByteArrayOutputStream timestampByteBuf = new ByteArrayOutputStream(20);
        byte[] yearBytes = fastGetSmallLongBytesForDate(year, 4);
        byte[] monthBytes = fastGetSmallLongBytesForDate(month, 2);
        byte[] dayBytes = fastGetSmallLongBytesForDate(day, 2);
        byte[] hourBytes = fastGetSmallLongBytesForDate(hour, 2);
        byte[] minuteBytes = fastGetSmallLongBytesForDate(minute, 2);
        byte[] secondBytes = fastGetSmallLongBytesForDate(second, 2);
        byte[] nanosecondBytes;
        String nanosecondString;
        String zeros = "000000000";

        if (secondPart == 0) {
            nanosecondBytes = null;
        } else {
            nanosecondString = Long.toString(secondPart);
            if (nanosecondString.length() <= 9) {
                // Add leading zeros
                nanosecondString = zeros.substring(0, (9 - nanosecondString.length())) +
                    nanosecondString;
            }

            // Truncate trailing zeros
            char[] nanosChar = new char[nanosecondString.length()];
            nanosecondString.getChars(0, nanosecondString.length(), nanosChar, 0);
            int truncIndex = 8;
            while (nanosChar[truncIndex] == '0') {
                truncIndex--;
            }

            nanosecondBytes = new String(nanosChar, 0, truncIndex + 1).getBytes();
        }

        if (isNeg) {
            timestampByteBuf.write('-');
        }
        timestampByteBuf.write(yearBytes);
        timestampByteBuf.write('-');
        timestampByteBuf.write(monthBytes);
        timestampByteBuf.write('-');
        timestampByteBuf.write(dayBytes);
        timestampByteBuf.write(' ');
        timestampByteBuf.write(hourBytes);
        timestampByteBuf.write(':');
        timestampByteBuf.write(minuteBytes);
        timestampByteBuf.write(':');
        timestampByteBuf.write(secondBytes);

        if (nanosecondBytes != null) {
            //  for nanosecond != 0
            int nanoLen = nanosecondBytes.length;
            timestampByteBuf.write('.');
            timestampByteBuf.write(nanosecondBytes);

            // append '0'
            if (scale > nanoLen) {
                for (int i = nanoLen; i < scale; i++) {
                    timestampByteBuf.write('0');
                }
            }
        } else if (scale > 0) {
            timestampByteBuf.write('.');
            // for nanosecond = 0 but scale > 0
            for (int i = 0; i < scale; i++) {
                timestampByteBuf.write('0');
            }
        }

        return timestampByteBuf.toByteArray();
    }

    public long toUnsignedLong() {
        return MySQLTimeConverter.datetimeToLong(this);
    }

    public long toPackedLong() {
        switch (this.sqlType) {
        case Types.DATE:
            return TimeStorage.writeDate(this);
        case Types.TIME:
            return TimeStorage.writeTime(this);
        case Types.TIMESTAMP:
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
        default:
            return TimeStorage.writeTimestamp(this);
        }
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    public boolean isNeg() {
        return isNeg;
    }

    public void setNeg(boolean neg) {
        isNeg = neg;
    }

    public long getYear() {
        return year;
    }

    public long getMonth() {
        return month;
    }

    public long getDay() {
        return day;
    }

    public long getHour() {
        return hour;
    }

    public long getMinute() {
        return minute;
    }

    public long getSecond() {
        return second;
    }

    public long getSecondPart() {
        return secondPart;
    }

    public void setYear(long year) {
        this.year = year;
    }

    public void setMonth(long month) {
        this.month = month;
    }

    public void setDay(long day) {
        this.day = day;
    }

    public void setHour(long hour) {
        this.hour = hour;
    }

    public void setMinute(long minute) {
        this.minute = minute;
    }

    public void setSecond(long second) {
        this.second = second;
    }

    public void setSecondPart(long secondPart) {
        this.secondPart = secondPart;
    }

    public TimeZone getTimezone() {
        return timezone;
    }

    public void setTimezone(TimeZone timezone) {
        this.timezone = timezone;
    }

    // clear all states.
    public void reset() {
        this.year = 0;
        this.month = 0;
        this.day = 0;
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
        this.secondPart = 0;
        this.isNeg = false;
        this.timezone = null;
    }
}

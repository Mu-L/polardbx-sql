package com.alibaba.polardbx.server.encdb.mask;

import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskAlgo;
import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskType;
import com.google.common.base.Strings;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomBigInt;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomDate;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomDecimal;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomDouble;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomFloat;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomInteger;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomMediumInt;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomSmallInt;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomTime;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomTimestamp;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomTinyInt;
import static com.alibaba.polardbx.server.encdb.mask.EncdbRandomGenerator.randomYear;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_DATE;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_DATETIME;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_DECIMAL;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_DOUBLE;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_FLOAT;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_INT24;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_LONG;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_LONGLONG;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_NEW_DECIMAL;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_SHORT;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_TIME;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_TIMESTAMP;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_TINY;
import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.FIELD_TYPE_YEAR;

/**
 * @author pangzhaoxing
 */
public class EncdbMaskUtil {

    public static char MASK_CHAR = '*';

    public static String MASK_STR() {
        return String.valueOf(MASK_CHAR);
    }

    public static Object mask(byte[] bytes, EncdbMaskAlgo algo, int mysqlType, String typeName, int precision,
                              int scale) {
        if (algo == null || algo.getMaskType() == EncdbMaskType.MASK_DATA_TYPE) {
            return dataTypeMask(mysqlType, typeName, precision, scale);
        }

        if (bytes == null) {
            return null;
        }

        switch (algo.getMaskType()) {
        case MASK_ALL:
            return allMask(new String(bytes, StandardCharsets.UTF_8));
        case MASK_FIX_POS:
            return fixPosMask(new String(bytes, StandardCharsets.UTF_8),
                Arrays.stream(algo.getParams()).mapToInt(i -> (int) i).toArray());
        case MASK_FIX_CHAR:
            return fixCharMask(new String(bytes, StandardCharsets.UTF_8), (String) algo.getParams()[0]);
        case MASK_EMAIL_PERSON:
            return emailPersonMask(new String(bytes, StandardCharsets.UTF_8));
        case MASK_EMAIL_PERSON_AND_COMPANY:
            return emailPersonAndCompanyMask(new String(bytes, StandardCharsets.UTF_8));
        case REPLACE_ALL:
            return allReplace(new String(bytes, StandardCharsets.UTF_8), (String) algo.getParams()[0]);
        case REPLACE_MAP:
            return mapReplace(new String(bytes, StandardCharsets.UTF_8), (String) algo.getParams()[0],
                (String) algo.getParams()[1]);
        case REPLACE_RANDOM:
            if (algo.getParams().length == 0) {
                return randomReplace(new String(bytes, StandardCharsets.UTF_8));
            } else if (algo.getParams().length == 1) {
                return randomReplace(new String(bytes, StandardCharsets.UTF_8), (String) algo.getParams()[0]);
            } else if (algo.getParams().length == 2) {
                return randomReplace(new String(bytes, StandardCharsets.UTF_8), (int) algo.getParams()[0],
                    (int) algo.getParams()[1]);
            } else {
                return randomReplace(new String(bytes, StandardCharsets.UTF_8), (int) algo.getParams()[0],
                    (int) algo.getParams()[1], (String) algo.getParams()[2]);
            }
        case REPLACE_NUMBER:
            if (algo.getParams().length == 1) {
                return numberReplace(new String(bytes, StandardCharsets.UTF_8), (int) algo.getParams()[0]);
            } else if (algo.getParams().length == 2) {
                return numberReplace(new String(bytes, StandardCharsets.UTF_8), (int) algo.getParams()[0],
                    (int) algo.getParams()[1]);
            }
        case TRANSFORM_NUMBER_ROUNDING:
            return numberRounding(Double.parseDouble(new String(bytes, StandardCharsets.UTF_8)));
        case TRANSFORM_DATE_ROUNDING:
            if (mysqlType == FIELD_TYPE_DATE) {
                return dateRounding(Date.valueOf(new String(bytes, StandardCharsets.UTF_8)),
                    (EncdbMaskType.DateRoundingLevel) algo.getParams()[0]);
            } else if (mysqlType == FIELD_TYPE_TIME) {
                return dateRounding(Time.valueOf(new String(bytes, StandardCharsets.UTF_8)),
                    (EncdbMaskType.DateRoundingLevel) algo.getParams()[0]);
            } else {
                return dateRounding(Timestamp.valueOf(new String(bytes, StandardCharsets.UTF_8)),
                    (EncdbMaskType.DateRoundingLevel) algo.getParams()[0]);
            }
        case TRANSFORM_STRING_LEFT_SHIFT:
            return leftShiftString(new String(bytes, StandardCharsets.UTF_8), (int) algo.getParams()[0]);
        case TRANSFORM_SHUFFLE:
            return shuffleString(new String(bytes, StandardCharsets.UTF_8));
        default:
            throw new UnsupportedOperationException(algo.getMaskType().name());
        }
    }

    public static Object dataTypeMask(int mysqlType, String typeName, int precision, int scale) {
        Object maskData;
        switch (mysqlType) {
        case FIELD_TYPE_TINY:
            maskData = randomTinyInt();
            break;
        case FIELD_TYPE_SHORT:
            if (typeName.toLowerCase().contains("year")) {
                maskData = randomYear();
            } else {
                maskData = randomSmallInt();
            }
            break;
        case FIELD_TYPE_INT24:
            maskData = randomMediumInt();
            break;
        case FIELD_TYPE_LONG:
            maskData = randomInteger();
            break;
        case FIELD_TYPE_LONGLONG:
            maskData = randomBigInt();
            break;
        case FIELD_TYPE_FLOAT:
            maskData = randomFloat();
            break;
        case FIELD_TYPE_DOUBLE:
            maskData = randomDouble();
            break;
        case FIELD_TYPE_NEW_DECIMAL:
        case FIELD_TYPE_DECIMAL:
            maskData = randomDecimal(precision, scale);
            break;
        case FIELD_TYPE_DATE:
            maskData = randomDate();
            break;
        case FIELD_TYPE_TIME:
            maskData = randomTime();
            break;
        case FIELD_TYPE_DATETIME:
        case FIELD_TYPE_TIMESTAMP:
            maskData = randomTimestamp();
            break;
        case FIELD_TYPE_YEAR:
            maskData = randomYear();
            break;
        default:
            maskData = "*";
        }
        return maskData;
    }

    public static String allMask(String str) {
        int len = str.length();
        return Strings.repeat(MASK_STR(), len);
    }

    public static String fixPosMask(String str, int[] idxArr) {
        if (str.length() == 0) {
            return str;
        }

        char[] chars = str.toCharArray();
        for (int i = 0; i < idxArr.length; i = i + 2) {
            int start = idxArr[i];
            int end = idxArr[i + 1];
            if (start > end) {
                continue;
            }
            start = start < 0 ? (start + str.length() + 1) : start;
            end = end < 0 ? (end + str.length() + 1) : end;
            if (start > end) {
                continue;
            }
            for (int j = start; j <= end; j++) {
                if (j > str.length()) {
                    break;
                }
                if (j < 1) {
                    continue;
                }
                chars[j - 1] = MASK_CHAR;
            }
        }
        return new String(chars);
    }

    public static String fixCharMask(String str, String oldChar) {
        String newChar = Strings.repeat(MASK_STR(), oldChar.length());
        return str.replace(oldChar, newChar);
    }

    public static String emailPersonMask(String str) {
        int index = str.lastIndexOf("@");
        return Strings.repeat(MASK_STR(), index) + str.substring(index);
    }

    public static String emailPersonAndCompanyMask(String str) {
        int index1 = str.lastIndexOf("@");
        int index2 = str.indexOf(".");
        return Strings.repeat(MASK_STR(), index1) + "@" + Strings.repeat(MASK_STR(), index2 - index1 - 1)
            + str.substring(index2);
    }

    public static String allReplace(String str, String newChar) {
        return newChar;
    }

    public static String mapReplace(String str, String oldChar, String newChar) {
        return str.replace(oldChar, newChar);
    }

    public static String randomReplace(String str) {
        return randomReplace(str, 1, str.length(), null);
    }

    public static String randomReplace(String str, String randChar) {
        char[] chars = new char[str.length()];
        for (int i = 0; i < str.length(); i++) {
            chars[i] = ThreadLocalRandom.current().nextBoolean() ?
                randChar.charAt(ThreadLocalRandom.current().nextInt(randChar.length())) : str.charAt(i);
        }
        return new String(chars);
    }

    public static String randomReplace(String str, int start, int end) {
        return randomReplace(str, start, end, null);
    }

    public static String randomReplace(String str, int start, int end, String randChar) {
        if (str.length() == 0) {
            return str;
        }
        start = Math.max(start, 1);
        end = Math.min(end, str.length());
        if (start > end) {
            throw new IllegalArgumentException("start should be smaller than end");
        }

        char[] chars = new char[end - start + 1];
        for (int i = 0; i < chars.length; i++) {
            if (randChar != null) {
                chars[i] = randChar.charAt(ThreadLocalRandom.current().nextInt(randChar.length()));
            } else {
                do {
                    chars[i] = (char) ThreadLocalRandom.current().nextInt(33, 126);
                } while (chars[i] == str.charAt(start + i - 1));
            }
        }
        String randStr = new String(chars);
        return str.substring(0, start - 1) + randStr + str.substring(end);
    }

    public static String numberReplace(String str, int integer) {
        integer = Math.abs(integer) % 10;
        char[] chars = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] >= '0' && chars[i] <= '9') {
                chars[i] = String.valueOf(integer).charAt(0);
            }
        }
        return new String(chars);
    }

    public static String numberReplace(String str, int lower, int upper) {
        int min = Math.min(lower, upper);
        int max = Math.max(lower, upper);
        int random = min + ThreadLocalRandom.current().nextInt(max - min + 1);
        return String.valueOf(random);
    }

    public static int numberRounding(double d) {
        return (int) d;
    }

    public static Timestamp dateRounding(Timestamp ts, EncdbMaskType.DateRoundingLevel level) {
        LocalDateTime localDateTime = ts.toLocalDateTime();
        switch (level) {
        case YEAR:
            return Timestamp.valueOf(LocalDateTime.of(localDateTime.getYear(), 1, 1, 0, 0, 0));
        case MONTH:
            return Timestamp.valueOf(LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), 1, 0, 0, 0));
        case DAY:
            return Timestamp.valueOf(
                LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), localDateTime.getDayOfMonth(), 0, 0,
                    0));
        case HOUR:
            return Timestamp.valueOf(
                LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), localDateTime.getDayOfMonth(),
                    localDateTime.getHour(), 0, 0));
        case MINUTE:
            return Timestamp.valueOf(
                LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), localDateTime.getDayOfMonth(),
                    localDateTime.getHour(), localDateTime.getMinute(), 0));
        case SECOND:
            return Timestamp.valueOf(
                LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), localDateTime.getDayOfMonth(),
                    localDateTime.getHour(), localDateTime.getMinute(), localDateTime.getSecond()));
        default:
            throw new UnsupportedOperationException(level.name());
        }
    }

    public static Date dateRounding(Date date, EncdbMaskType.DateRoundingLevel level) {
        switch (level) {
        case YEAR:
            return new Date(date.getYear(), 0, 1);
        case MONTH:
            return new Date(date.getYear(), date.getMonth(), 1);
        case DAY:
        case HOUR:
        case MINUTE:
        case SECOND:
            return date;
        default:
            throw new UnsupportedOperationException(level.name());
        }
    }

    public static Time dateRounding(Time time, EncdbMaskType.DateRoundingLevel level) {
        switch (level) {
        case YEAR:
        case MONTH:
        case DAY:
            return new Time(0, 0, 0);
        case HOUR:
            return new Time(time.getHours(), 0, 0);
        case MINUTE:
            return new Time(time.getHours(), time.getMinutes(), 0);
        case SECOND:
            return time;
        default:
            throw new UnsupportedOperationException(level.name());
        }
    }

    public static String leftShiftString(String str, int shift) {
        if (shift < 0) {
            throw new IllegalArgumentException("shift should be positive");
        }
        int len = str.length();
        if (shift >= len) {
            return "";
        } else {
            return str.substring(shift);
        }
    }

    public static String shuffleString(String str) {
        char[] chars = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            int j = ThreadLocalRandom.current().nextInt(chars.length);
            char tmp = chars[i];
            chars[i] = chars[j];
            chars[j] = tmp;
        }
        return new String(chars);
    }

}

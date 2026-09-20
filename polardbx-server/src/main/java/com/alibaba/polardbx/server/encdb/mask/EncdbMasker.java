package com.alibaba.polardbx.server.encdb.mask;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.polardbx.server.executor.utils.MysqlDefs.*;

/**
 * @author pangzhaoxing
 */
public class EncdbMasker {

    public static byte[] maskData(byte[] data, int mysqlType, int precision, int scale) {
        switch (mysqlType) {
        case FIELD_TYPE_TINY:
            return maskTinyInt(data);
        case FIELD_TYPE_SHORT:
            return maskSmallInt(data);
        case FIELD_TYPE_INT24:
            return maskMediumInt(data);
        case FIELD_TYPE_LONG:
            return maskInt(data);
        case FIELD_TYPE_LONGLONG:
            return maskBigInt(data);
        case FIELD_TYPE_FLOAT:
            return maskFloat(data);
        case FIELD_TYPE_DOUBLE:
            return maskDouble(data);
        case FIELD_TYPE_DECIMAL:
            return maskDecimal(data, precision, scale);
        case FIELD_TYPE_DATE:
            return maskDate(data);
        case FIELD_TYPE_TIME:
            return maskTime(data);
        case FIELD_TYPE_DATETIME:
            return maskDatetime(data);
        case FIELD_TYPE_TIMESTAMP:
            return maskTimestamp(data);
        case FIELD_TYPE_YEAR:
            return maskYear(data);
        default:
            return maskOther(data);
        }
    }

    public static byte[] maskFloat(byte[] data) {
        double randomFloat =
            Float.MIN_VALUE + ThreadLocalRandom.current().nextFloat() * (Float.MAX_VALUE - Float.MIN_VALUE);
        randomFloat = ThreadLocalRandom.current().nextBoolean() ? -randomFloat : randomFloat;
        return DataTypes.BytesType.convertFrom(randomFloat);
    }

    public static byte[] maskDouble(byte[] data) {
        double randomDouble =
            Double.MIN_VALUE + ThreadLocalRandom.current().nextDouble() * (Double.MAX_VALUE - Double.MIN_VALUE);
        randomDouble = ThreadLocalRandom.current().nextBoolean() ? -randomDouble : randomDouble;
        return DataTypes.BytesType.convertFrom(randomDouble);
    }

    public static byte[] maskDecimal(byte[] data, int precision, int scale) {
        //TODO： integerPart and fractionalPart has limitation
        BigDecimal integerPart =
            new BigDecimal(ThreadLocalRandom.current().nextLong((long) Math.pow(10, precision - scale)));
        BigDecimal fractionalPart = new BigDecimal(ThreadLocalRandom.current().nextLong((long) Math.pow(10, scale)));
        BigDecimal randomDecimal = integerPart.add(fractionalPart.movePointLeft(scale));
        // 使用 MathContext 限制精度
        randomDecimal = randomDecimal.round(new MathContext(precision));
        return DataTypes.BytesType.convertFrom(randomDecimal);
    }

    public static byte[] maskDate(byte[] data) {
        long startMill = java.sql.Date.valueOf("1900-01-01").getTime();
        long endMill = java.sql.Date.valueOf("9999-12-31").getTime();
        long randomMillis = startMill + (long) (ThreadLocalRandom.current().nextDouble() * (endMill - startMill));
        Date randomDate = new Date(randomMillis);
        return DataTypes.BytesType.convertFrom(randomDate);
    }

    public static byte[] maskTime(byte[] data) {
        int hours = ThreadLocalRandom.current().nextInt(24); // 0 - 23
        int minutes = ThreadLocalRandom.current().nextInt(60); // 0 - 59
        int seconds = ThreadLocalRandom.current().nextInt(60); // 0 - 59
        Time randomTime = new Time(hours, minutes, seconds);
        return DataTypes.BytesType.convertFrom(randomTime);
    }

    public static byte[] maskDatetime(byte[] data) {
        long startMill = Timestamp.valueOf("1900-01-01 00:00:00").getTime();
        long endMill = Timestamp.valueOf("9999-12-31 23:59:59").getTime();
        long randomMillis = startMill + (long) (ThreadLocalRandom.current().nextDouble() * (endMill - startMill));
        Timestamp randomDatetime = new Timestamp(randomMillis);
        return DataTypes.BytesType.convertFrom(randomDatetime);
    }

    public static byte[] maskTimestamp(byte[] data) {
        long startMill = Timestamp.valueOf("1900-01-01 00:00:00").getTime();
        long endMill = Timestamp.valueOf("9999-12-31 23:59:59").getTime();
        long randomMillis = startMill + (long) (ThreadLocalRandom.current().nextDouble() * (endMill - startMill));
        Timestamp randomDatetime = new Timestamp(randomMillis);
        return DataTypes.BytesType.convertFrom(randomDatetime);
    }

    public static byte[] maskYear(byte[] data) {
        long randYear = ThreadLocalRandom.current().nextLong(1990, 10000);
        return DataTypes.BytesType.convertFrom(randYear);
    }

    public static byte[] maskTinyInt(byte[] data) {
        return randomBytes(1);
    }

    public static byte[] maskSmallInt(byte[] data) {
        return randomBytes(2);
    }

    public static byte[] maskMediumInt(byte[] data) {
        return randomBytes(3);
    }

    public static byte[] maskInt(byte[] data) {
        return randomBytes(4);
    }

    public static byte[] maskBigInt(byte[] data) {
        return randomBytes(8);
    }

    public static byte[] randomBytes(int byesLen) {
        byte[] maskData = new byte[byesLen];
        ThreadLocalRandom.current().nextBytes(maskData);
        return maskData;
    }

    public static byte[] maskOther(byte[] data) {
        String s = "*";
        return DataTypes.BytesType.convertFrom(s);
    }

}

package com.alibaba.polardbx.server.encdb.mask;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author pangzhaoxing
 */
public class EncdbRandomGenerator {

    private static final float MAX_FLOAT = 999999f;
    private static final double MAX_DOUBLE = 99999999999d;

    public static float randomFloat() {
        float randomFloat = Float.MIN_VALUE + ThreadLocalRandom.current().nextFloat() * MAX_FLOAT;
        randomFloat = ThreadLocalRandom.current().nextBoolean() ? -randomFloat : randomFloat;
        return randomFloat;
    }

    public static double randomDouble() {
        double randomDouble = Double.MIN_VALUE + ThreadLocalRandom.current().nextDouble() * MAX_DOUBLE;
        randomDouble = ThreadLocalRandom.current().nextBoolean() ? -randomDouble : randomDouble;
        return randomDouble;
    }

    public static BigDecimal randomDecimal(int precision, int scale) {
        //TODO： integerPart and fractionalPart has limitation
        BigDecimal integerPart =
            new BigDecimal(ThreadLocalRandom.current().nextLong((long) Math.pow(10, precision - scale)));
        BigDecimal fractionalPart = new BigDecimal(ThreadLocalRandom.current().nextLong((long) Math.pow(10, scale)));
        BigDecimal randomDecimal = integerPart.add(fractionalPart.movePointLeft(scale));
        // 使用 MathContext 限制精度
        randomDecimal = randomDecimal.round(new MathContext(precision));
        return randomDecimal;
    }

    public static Date randomDate() {
        long startMill = java.sql.Date.valueOf("1900-01-01").getTime();
        long endMill = java.sql.Date.valueOf("9999-12-31").getTime();
        long randomMillis = startMill + (long) (ThreadLocalRandom.current().nextDouble() * (endMill - startMill));
        Date randomDate = new Date(randomMillis);
        return randomDate;
    }

    public static Time randomTime() {
        int hours = ThreadLocalRandom.current().nextInt(24); // 0 - 23
        int minutes = ThreadLocalRandom.current().nextInt(60); // 0 - 59
        int seconds = ThreadLocalRandom.current().nextInt(60); // 0 - 59
        Time randomTime = new Time(hours, minutes, seconds);
        return randomTime;
    }

    public static Timestamp randomTimestamp() {
        long startMill = Timestamp.valueOf("1900-01-01 00:00:00").getTime();
        long endMill = Timestamp.valueOf("9999-12-31 23:59:59").getTime();
        long randomMillis = startMill + (long) (ThreadLocalRandom.current().nextDouble() * (endMill - startMill));
        Timestamp randomDatetime = new Timestamp(randomMillis);
        return randomDatetime;
    }

    public static long randomYear() {
        long randYear = ThreadLocalRandom.current().nextLong(1990, 10000);
        return randYear;
    }

    public static byte randomTinyInt() {
        return (byte) ThreadLocalRandom.current().nextInt(-128, 128);
    }

    public static short randomUTinyInt() {
        return (short) ThreadLocalRandom.current().nextInt(0, 256);
    }

    public static short randomSmallInt() {
        return (short) ThreadLocalRandom.current().nextInt(-32768, 32768);
    }

    public static int randomUSmallInt() {
        return ThreadLocalRandom.current().nextInt(0, 65535);
    }

    public static int randomMediumInt() {
        return ThreadLocalRandom.current().nextInt(-8388608, 8388608);
    }

    public static int randomUMediumInt() {
        return ThreadLocalRandom.current().nextInt(0, 16777216);
    }

    public static int randomInteger() {
        return ThreadLocalRandom.current().nextInt();
    }

    public static long randomUInteger() {
        return ThreadLocalRandom.current().nextLong(0, 4294967296L);
    }

    public static long randomBigInt() {
        return ThreadLocalRandom.current().nextLong();
    }

}

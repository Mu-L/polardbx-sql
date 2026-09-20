package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;

public class MemoryCounterUtils {
    private static final int STRING_INSTANCE_SIZE = ClassLayout.parseClass(String.class).instanceSize();

    private static final int BIG_INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(BigInteger.class).instanceSize();
    private static final int BIG_DECIMAL_INSTANCE_SIZE = ClassLayout.parseClass(BigDecimal.class).instanceSize();

    private static final  int BYTE_INSTANCE_SIZE = ClassLayout.parseClass(Byte.class).instanceSize();
    private static final int SHORT_INSTANCE_SIZE = ClassLayout.parseClass(Short.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();
    private static final int LONG_INSTANCE_SIZE = ClassLayout.parseClass(Long.class).instanceSize();
    private static final int FLOAT_INSTANCE_SIZE = ClassLayout.parseClass(Float.class).instanceSize();
    private static final int DOUBLE_INSTANCE_SIZE = ClassLayout.parseClass(Double.class).instanceSize();

    private static final int TIME_INSTANCE_SIZE = ClassLayout.parseClass(Time.class).instanceSize();
    private static final int DATE_INSTANCE_SIZE = ClassLayout.parseClass(Date.class).instanceSize();
    private static final int DATE_UTIL_INSTANCE_SIZE = ClassLayout.parseClass(java.util.Date.class).instanceSize();
    private static final int TIMESTAMP_INSTANCE_SIZE = ClassLayout.parseClass(Timestamp.class).instanceSize();

    private static ImmutableMap<Class, MemoryCounter> memoryCounterMap;

    private static HashMap<Class<?>, MemoryCounter<?>> hashMap = new HashMap<>();
    private static <T> void putMap(Class<T> clazz, MemoryCounter<T> memoryCounter) {
       hashMap.put(clazz, memoryCounter);
    }

    static {
        // normal number
        putMap(Byte.class, e -> BYTE_INSTANCE_SIZE);
        putMap(Short.class, e -> SHORT_INSTANCE_SIZE);
        putMap(Integer.class, e -> INTEGER_INSTANCE_SIZE);
        putMap(Long.class, e -> LONG_INSTANCE_SIZE);
        putMap(Float.class, e -> FLOAT_INSTANCE_SIZE);
        putMap(Double.class, e -> DOUBLE_INSTANCE_SIZE);

        // uint 64
        putMap(UInt64.class, e -> e.getMemoryUsage());

        // big integer
        putMap(BigInteger.class, e -> {
            int magLength = (e.bitLength() + 31) / 32;
            return BIG_INTEGER_INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOfIntArray(magLength));
        });

        // time
        putMap(Time.class, e -> TIME_INSTANCE_SIZE);
        putMap(OriginalTime.class, e -> e.getMemoryUsage());

        // date
        putMap(Date.class, e -> DATE_INSTANCE_SIZE);
        putMap(java.util.Date.class, e -> DATE_UTIL_INSTANCE_SIZE);
        putMap(OriginalDate.class, e -> e.getMemoryUsage());

        // time stamp
        putMap(Timestamp.class, e -> TIMESTAMP_INSTANCE_SIZE);
        putMap(OriginalTimestamp.class, e -> e.getMemoryUsage());

        // string
        putMap(String.class, e -> STRING_INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOfCharArray(e.length())));

        memoryCounterMap = ImmutableMap.copyOf(hashMap);
    }

    public static <T> MemoryCounter<T> getMemoryCounter(Class<T> clazz) {
        return memoryCounterMap.get(clazz);
    }

    public static long getMemoryUsage(Object object) {
        if (object == null) {
            return 0L;
        }
        MemoryCounter memoryCounter = memoryCounterMap.get(object.getClass());
        if (memoryCounter == null) {
            throw new UnsupportedOperationException(object.getClass().getName() + " not support memory counter");
        }
        return memoryCounter.getMemoryUsage(object);
    }
}

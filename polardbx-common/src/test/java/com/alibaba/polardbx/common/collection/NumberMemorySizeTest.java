package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

public class NumberMemorySizeTest {
    /**
     * Test the memory size of Byte instances.
     */
    @Test
    public void testByte() {
        // Test with different Byte values
        Byte[] bytes = {
            Byte.valueOf((byte) 0),
            Byte.valueOf(Byte.MAX_VALUE),
            Byte.valueOf(Byte.MIN_VALUE)
        };

        for (Byte b : bytes) {
            long fastSize = FastMemoryCounter.sizeOfNumber(b);
            long graphSize = GraphLayout.parseInstance(b).totalSize();
            assertEquals("Byte size mismatch for value: " + b, graphSize, fastSize);
        }
    }

    /**
     * Test the memory size of Short instances.
     */
    @Test
    public void testShort() {
        // Test with different Short values
        Short[] shorts = {
            Short.valueOf((short) 0),
            Short.valueOf(Short.MAX_VALUE),
            Short.valueOf(Short.MIN_VALUE)
        };

        for (Short s : shorts) {
            long fastSize = FastMemoryCounter.sizeOfNumber(s);
            long graphSize = GraphLayout.parseInstance(s).totalSize();
            assertEquals("Short size mismatch for value: " + s, graphSize, fastSize);
        }
    }

    /**
     * Test the memory size of Integer instances.
     */
    @Test
    public void testInteger() {
        // Test with different Integer values
        Integer[] integers = {
            Integer.valueOf(0),
            Integer.valueOf(Integer.MAX_VALUE),
            Integer.valueOf(Integer.MIN_VALUE),
            Integer.valueOf(123456)
        };

        for (Integer i : integers) {
            long fastSize = FastMemoryCounter.sizeOfNumber(i);
            long graphSize = GraphLayout.parseInstance(i).totalSize();
            assertEquals("Integer size mismatch for value: " + i, graphSize, fastSize);
        }
    }

    /**
     * Test the memory size of Long instances.
     */
    @Test
    public void testLong() {
        // Test with different Long values
        Long[] longs = {
            Long.valueOf(0L),
            Long.valueOf(Long.MAX_VALUE),
            Long.valueOf(Long.MIN_VALUE),
            Long.valueOf(123456789L)
        };

        for (Long l : longs) {
            long fastSize = FastMemoryCounter.sizeOfNumber(l);
            long graphSize = GraphLayout.parseInstance(l).totalSize();
            assertEquals("Long size mismatch for value: " + l, graphSize, fastSize);
        }
    }

    /**
     * Test the memory size of Float instances.
     */
    @Test
    public void testFloat() {
        // Test with different Float values
        Float[] floats = {
            Float.valueOf(0.0f),
            Float.valueOf(Float.MAX_VALUE),
            Float.valueOf(Float.MIN_VALUE),
            Float.valueOf(12345.67f)
        };

        for (Float f : floats) {
            long fastSize = FastMemoryCounter.sizeOfNumber(f);
            long graphSize = GraphLayout.parseInstance(f).totalSize();
            assertEquals("Float size mismatch for value: " + f, graphSize, fastSize);
        }
    }

    /**
     * Test the memory size of Double instances.
     */
    @Test
    public void testDouble() {
        // Test with different Double values
        Double[] doubles = {
            Double.valueOf(0.0),
            Double.valueOf(Double.MAX_VALUE),
            Double.valueOf(Double.MIN_VALUE),
            Double.valueOf(123456.789)
        };

        for (Double d : doubles) {
            long fastSize = FastMemoryCounter.sizeOfNumber(d);
            long graphSize = GraphLayout.parseInstance(d).totalSize();
            assertEquals("Double size mismatch for value: " + d, graphSize, fastSize);
        }
    }

    /**
     * Test the memory size of BigInteger instances with different constructors.
     */
    @Test
    public void testBigInteger() {
        // Test with different BigInteger constructors
        BigInteger[] bigIntegers = new BigInteger[] {
            BigInteger.ZERO,
            BigInteger.ONE,
            new BigInteger("12345678901234567890"),
            new BigInteger("-98765432109876543210"),
            new BigInteger("FFFFFFFF", 16) // Hexadecimal constructor
        };

        for (BigInteger bi : bigIntegers) {
            long fastSize = FastMemoryCounter.sizeOfBigInteger(bi);
            long graphSize = GraphLayout.parseInstance(bi).totalSize();
            assertEquals("BigInteger size mismatch for value: " + bi, graphSize, fastSize);
        }
    }
}

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for Slice.fill() method and fillLong() utility
 */
public class SliceFillTest {

    @Test
    public void testFillWithZero() {
        // Test filling with zero byte
        Slice slice = Slices.allocate(20);
        slice.fill((byte) 0x00);

        for (int i = 0; i < 20; i++) {
            Assert.assertEquals((byte) 0x00, slice.getByte(i));
        }
    }

    @Test
    public void testFillWithFF() {
        // Test filling with 0xFF
        Slice slice = Slices.allocate(20);
        slice.fill((byte) 0xFF);

        for (int i = 0; i < 20; i++) {
            Assert.assertEquals((byte) 0xFF, slice.getByte(i));
        }
    }

    @Test
    public void testFillWithArbitraryValue() {
        // Test filling with arbitrary byte value
        Slice slice = Slices.allocate(20);
        slice.fill((byte) 0x12);

        for (int i = 0; i < 20; i++) {
            Assert.assertEquals((byte) 0x12, slice.getByte(i));
        }
    }

    @Test
    public void testFillWithNegativeValue() {
        // Test filling with negative byte value
        Slice slice = Slices.allocate(20);
        slice.fill((byte) -1);

        for (int i = 0; i < 20; i++) {
            Assert.assertEquals((byte) -1, slice.getByte(i));
        }
    }

    @Test
    public void testFillSmallSlice() {
        // Test filling slice smaller than 8 bytes (SIZE_OF_LONG)
        Slice slice = Slices.allocate(5);
        slice.fill((byte) 0xAB);

        for (int i = 0; i < 5; i++) {
            Assert.assertEquals((byte) 0xAB, slice.getByte(i));
        }
    }

    @Test
    public void testFillExactlyOneLong() {
        // Test filling slice of exactly 8 bytes
        Slice slice = Slices.allocate(8);
        slice.fill((byte) 0xCD);

        for (int i = 0; i < 8; i++) {
            Assert.assertEquals((byte) 0xCD, slice.getByte(i));
        }
    }

    @Test
    public void testFillMultipleLongs() {
        // Test filling slice of multiple longs (16 bytes)
        Slice slice = Slices.allocate(16);
        slice.fill((byte) 0x34);

        for (int i = 0; i < 16; i++) {
            Assert.assertEquals((byte) 0x34, slice.getByte(i));
        }
    }

    @Test
    public void testFillNonAlignedSize() {
        // Test filling slice with size not aligned to 8 bytes
        Slice slice = Slices.allocate(25);  // 3 longs + 1 byte
        slice.fill((byte) 0x56);

        for (int i = 0; i < 25; i++) {
            Assert.assertEquals((byte) 0x56, slice.getByte(i));
        }
    }

    @Test
    public void testFillLargeSlice() {
        // Test filling a large slice
        Slice slice = Slices.allocate(1000);
        slice.fill((byte) 0x78);

        // Check first, middle, and last bytes
        Assert.assertEquals((byte) 0x78, slice.getByte(0));
        Assert.assertEquals((byte) 0x78, slice.getByte(500));
        Assert.assertEquals((byte) 0x78, slice.getByte(999));

        // Verify all bytes
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals((byte) 0x78, slice.getByte(i));
        }
    }

    @Test
    public void testFillEmptySlice() {
        // Test filling empty slice (should not throw exception)
        Slice slice = Slices.allocate(0);
        slice.fill((byte) 0x99);
        // No assertion needed, just verify it doesn't throw
    }

    @Test
    public void testFillOverwritesExistingData() {
        // Test that fill overwrites existing data
        Slice slice = Slices.allocate(20);

        // Set some initial data
        for (int i = 0; i < 20; i++) {
            slice.setByte(i, i);
        }

        // Fill with new value
        slice.fill((byte) 0xEE);

        // Verify all bytes are now 0xEE
        for (int i = 0; i < 20; i++) {
            Assert.assertEquals((byte) 0xEE, slice.getByte(i));
        }
    }

    @Test
    public void testFillLongDirectly() throws Exception {
        // Direct test of fillLong method using reflection
        // This verifies the fillLong utility method itself

        java.lang.reflect.Method fillLongMethod = Slice.class.getDeclaredMethod("fillLong", byte.class);
        fillLongMethod.setAccessible(true);

        // Test with 0x12
        long result1 = (Long) fillLongMethod.invoke(null, (byte) 0x12);
        Assert.assertEquals(0x1212121212121212L, result1);

        // Test with 0x00
        long result2 = (Long) fillLongMethod.invoke(null, (byte) 0x00);
        Assert.assertEquals(0x0000000000000000L, result2);

        // Test with 0xFF
        long result3 = (Long) fillLongMethod.invoke(null, (byte) 0xFF);
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result3);

        // Test with 0xAB
        long result4 = (Long) fillLongMethod.invoke(null, (byte) 0xAB);
        Assert.assertEquals(0xABABABABABABABABL, result4);

        // Test with negative value -1 (0xFF in unsigned)
        long result5 = (Long) fillLongMethod.invoke(null, (byte) -1);
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result5);
    }

    @Test
    public void testFillVerifyLongBoundary() {
        // Specifically test the boundary between long-aligned and byte-by-byte filling
        // Size 17 = 2 longs (16 bytes) + 1 byte
        Slice slice = Slices.allocate(17);
        slice.fill((byte) 0xCC);

        // Verify all 17 bytes
        for (int i = 0; i < 17; i++) {
            Assert.assertEquals("Byte at index " + i + " should be 0xCC",
                (byte) 0xCC, slice.getByte(i));
        }
    }

    @Test
    public void testFillDifferentValues() {
        // Test multiple fill operations with different values
        Slice slice = Slices.allocate(10);

        // Fill with first value
        slice.fill((byte) 0x11);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((byte) 0x11, slice.getByte(i));
        }

        // Fill with second value
        slice.fill((byte) 0x22);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((byte) 0x22, slice.getByte(i));
        }

        // Fill with third value
        slice.fill((byte) 0x33);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((byte) 0x33, slice.getByte(i));
        }
    }
}

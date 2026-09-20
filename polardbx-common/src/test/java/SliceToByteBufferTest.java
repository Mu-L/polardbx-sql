import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Unit tests for Slice.toByteBuffer() method
 */
public class SliceToByteBufferTest {

    @Test
    public void testToByteBufferWithByteArrayBase() {
        // Test normal case with byte array base
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        Slice slice = Slices.wrappedBuffer(data);

        ByteBuffer buffer = slice.toByteBuffer(0, 5);

        Assert.assertNotNull(buffer);
        Assert.assertEquals(5, buffer.remaining());
        Assert.assertEquals(1, buffer.get(0));
        Assert.assertEquals(5, buffer.get(4));
    }

    @Test
    public void testToByteBufferWithZeroLength() {
        // Test zero length case with byte array base
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        Slice slice = Slices.wrappedBuffer(data);

        ByteBuffer buffer = slice.toByteBuffer(0, 0);

        Assert.assertNotNull(buffer);
        Assert.assertEquals(0, buffer.remaining());
        Assert.assertEquals(0, buffer.position());
        Assert.assertEquals(0, buffer.limit());
    }

    @Test
    public void testToByteBufferWithPartialSlice() {
        // Test with partial slice
        byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        Slice slice = Slices.wrappedBuffer(data);

        ByteBuffer buffer = slice.toByteBuffer(2, 5);

        Assert.assertNotNull(buffer);
        Assert.assertEquals(5, buffer.remaining());
        // ByteBuffer.wrap sets position=offset, so we need to access from position
        Assert.assertEquals(3, buffer.get(buffer.position()));  // index 2 in original array
        Assert.assertEquals(7, buffer.get(buffer.position() + 4));  // index 6 in original array
    }

    @Test
    public void testToByteBufferWithSingleByte() {
        // Test with single byte
        byte[] data = new byte[] {42};
        Slice slice = Slices.wrappedBuffer(data);

        ByteBuffer buffer = slice.toByteBuffer(0, 1);

        Assert.assertNotNull(buffer);
        Assert.assertEquals(1, buffer.remaining());
        Assert.assertEquals(42, buffer.get(0));
    }

    @Test
    public void testToByteBufferAtDifferentIndices() {
        // Test at different starting indices
        byte[] data = new byte[] {10, 20, 30, 40, 50, 60, 70, 80};
        Slice slice = Slices.wrappedBuffer(data);

        // Test at index 0
        ByteBuffer buffer1 = slice.toByteBuffer(0, 3);
        Assert.assertEquals(10, buffer1.get(buffer1.position()));
        Assert.assertEquals(20, buffer1.get(buffer1.position() + 1));
        Assert.assertEquals(30, buffer1.get(buffer1.position() + 2));

        // Test at index 5
        ByteBuffer buffer2 = slice.toByteBuffer(5, 3);
        Assert.assertEquals(60, buffer2.get(buffer2.position()));
        Assert.assertEquals(70, buffer2.get(buffer2.position() + 1));
        Assert.assertEquals(80, buffer2.get(buffer2.position() + 2));
    }

    @Test
    public void testToByteBufferZeroLengthAtDifferentIndices() {
        // Test zero length at different indices
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        Slice slice = Slices.wrappedBuffer(data);

        ByteBuffer buffer1 = slice.toByteBuffer(0, 0);
        Assert.assertEquals(0, buffer1.remaining());

        ByteBuffer buffer2 = slice.toByteBuffer(2, 0);
        Assert.assertEquals(0, buffer2.remaining());

        ByteBuffer buffer3 = slice.toByteBuffer(5, 0);
        Assert.assertEquals(0, buffer3.remaining());
    }

    @Test
    public void testToByteBufferWithEmptySlice() {
        // Test with empty slice
        Slice slice = Slices.allocate(0);

        ByteBuffer buffer = slice.toByteBuffer(0, 0);

        Assert.assertNotNull(buffer);
        Assert.assertEquals(0, buffer.remaining());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testToByteBufferWithInvalidIndex() {
        // Test with invalid index (should throw exception)
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        Slice slice = Slices.wrappedBuffer(data);

        // This should throw IndexOutOfBoundsException
        slice.toByteBuffer(6, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testToByteBufferWithInvalidLength() {
        // Test with invalid length (should throw exception)
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        Slice slice = Slices.wrappedBuffer(data);

        // This should throw IndexOutOfBoundsException
        slice.toByteBuffer(0, 10);
    }

    @Test
    public void testToByteBufferEntireSlice() {
        // Test converting entire slice to ByteBuffer
        Slice slice = Slices.allocate(100);
        for (int i = 0; i < 100; i++) {
            slice.setByte(i, i % 256);
        }

        ByteBuffer buffer = slice.toByteBuffer(0, 100);

        Assert.assertNotNull(buffer);
        Assert.assertEquals(100, buffer.remaining());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals((byte) (i % 256), buffer.get(i));
        }
    }

    @Test
    public void testToByteBufferMultipleTimes() {
        // Test calling toByteBuffer multiple times on same slice
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        Slice slice = Slices.wrappedBuffer(data);

        ByteBuffer buffer1 = slice.toByteBuffer(0, 5);
        ByteBuffer buffer2 = slice.toByteBuffer(0, 5);

        Assert.assertNotNull(buffer1);
        Assert.assertNotNull(buffer2);
        Assert.assertEquals(buffer1.remaining(), buffer2.remaining());

        // Verify both buffers have same content
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(buffer1.get(i), buffer2.get(i));
        }
    }

    @Test
    public void testToByteBufferNoParam() {
        // Test toByteBuffer() without parameters (entire slice)
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        Slice slice = Slices.wrappedBuffer(data);

        ByteBuffer buffer = slice.toByteBuffer();

        Assert.assertNotNull(buffer);
        Assert.assertEquals(5, buffer.remaining());
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(data[i], buffer.get(i));
        }
    }

    @Test
    public void testToByteBufferEmptySliceNoParam() {
        // Test toByteBuffer() on empty slice without parameters
        Slice slice = Slices.allocate(0);

        ByteBuffer buffer = slice.toByteBuffer();

        Assert.assertNotNull(buffer);
        Assert.assertEquals(0, buffer.remaining());
    }

    @Test(expected = RuntimeException.class)
    public void testToByteBufferWithNonByteArrayBaseNonZeroLength() throws Exception {
        // Test with non-byte[] base and non-zero length
        // This should throw RuntimeException as the base is not byte[] and length > 0
        // We use reflection to create a Slice with non-byte[] base (e.g., int[])

        int[] intArray = new int[] {1, 2, 3, 4, 5};

        // Use reflection to call the package-private constructor
        // Slice(Object base, long address, int size, int retainedSize, Object reference)
        java.lang.reflect.Constructor<Slice> constructor = Slice.class.getDeclaredConstructor(
            Object.class, long.class, int.class, int.class, Object.class
        );
        constructor.setAccessible(true);

        // Create a Slice with int[] as base
        // Note: address must be > 0, size must be > 0
        Slice slice = constructor.newInstance(intArray, 16L, 20, 100, null);

        // This should throw RuntimeException: "Failed to convert to byteBuffer"
        slice.toByteBuffer(0, 5);
    }

    @Test
    public void testToByteBufferWithNonByteArrayBaseZeroLength() throws Exception {
        // Test with non-byte[] base but zero length
        // This should NOT throw exception as length == 0 is handled specially

        int[] intArray = new int[] {1, 2, 3, 4, 5};

        // Use reflection to call the package-private constructor
        java.lang.reflect.Constructor<Slice> constructor = Slice.class.getDeclaredConstructor(
            Object.class, long.class, int.class, int.class, Object.class
        );
        constructor.setAccessible(true);

        // Create a Slice with int[] as base
        Slice slice = constructor.newInstance(intArray, 16L, 20, 100, null);

        // This should return an empty ByteBuffer without throwing exception
        ByteBuffer buffer = slice.toByteBuffer(0, 0);
        Assert.assertNotNull(buffer);
        Assert.assertEquals(0, buffer.remaining());
    }
}

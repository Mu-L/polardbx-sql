package com.alibaba.polardbx.executor.function.calc;

import com.alibaba.polardbx.common.oss.blob.BlobRef;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import java.nio.charset.StandardCharsets;

public class FetchBlobTest {

    @Test
    public void testDecodeBlobRefNull() {
        Assert.assertNull(FetchBlob.decodeBlobRef(null));
    }

    @Test
    public void testDecodeBlobRefEmptyByteArray() {
        Assert.assertNull(FetchBlob.decodeBlobRef(new byte[0]));
    }

    @Test
    public void testDecodeBlobRefInvalidHex() {
        Assert.assertNull(FetchBlob.decodeBlobRef("not_a_valid_hex"));
    }

    @Test
    public void testDecodeBlobRefEmptyString() {
        long[] result = FetchBlob.decodeBlobRef("");
        Assert.assertNull(result);
    }
}

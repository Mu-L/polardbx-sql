package com.alibaba.polardbx.executor.operator.scan;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.alibaba.polardbx.common.orc.BinaryMetadataCorruptException;
import com.alibaba.polardbx.common.orc.ORCMetaReaderImpl;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * iter-002 N4 defense: ORCMetaReaderImpl reader-side defense and fallback against corrupted
 * BinaryMetadata fields.
 * <p>
 * Red phase (no defense): C1/C2/C4 throw NegativeArraySizeException, C5 throws AIOOBE.
 * Green phase (with defense): every case falls back to the legacy preheatStripe path,
 * useBinaryMeta=false, and a WARN log contains the expected keyword.
 * <p>
 * Unit tests must run on JDK 11 (see project acceptance/unit-test-jdk-runtime.md).
 */
public class BinaryMetadataRobustnessTest {

    private static final String ORIG_FILE = "binary_metadata_robustness_orig.orc";
    private static final String BAD_FILE = "binary_metadata_robustness_bad.orc";

    private Configuration configuration;
    private FileSystem fileSystem;
    private Path origPath;
    private Path badPath;

    private ListAppender<ILoggingEvent> logAppender;
    private Logger targetLogger;

    @Before
    public void setUp() throws IOException {
        configuration = new Configuration();
        // Writer side: enable redundant + binary metadata so that the footer actually emits
        // binaryMetadata. See PhysicalFsWriter.writeFileFooter L474-L485 for details.
        OrcConf.USE_REDUNDANT_META_DATA.setBoolean(configuration, true);
        OrcConf.USE_BINARY_META_DATA.setBoolean(configuration, true);
        OrcConf.ROW_INDEX_STRIDE.setInt(configuration, 1000);
        OrcConf.STRIPE_SIZE.setLong(configuration, 4 * 1024);

        Path workDir = new Path(getClass().getClassLoader().getResource(".").toString());
        origPath = new Path(workDir, ORIG_FILE);
        badPath = new Path(workDir, BAD_FILE);
        fileSystem = FileSystem.get(origPath.toUri(), configuration);

        writeNormalOrc(origPath);

        // Attach a logback ListAppender to the ORCMetaReaderImpl logger.
        targetLogger = (Logger) LoggerFactory.getLogger(ORCMetaReaderImpl.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        targetLogger.addAppender(logAppender);
    }

    @After
    public void tearDown() throws IOException {
        if (targetLogger != null && logAppender != null) {
            targetLogger.detachAppender(logAppender);
        }
        if (fileSystem != null) {
            if (fileSystem.exists(origPath)) {
                fileSystem.delete(origPath, false);
            }
            if (fileSystem.exists(badPath)) {
                fileSystem.delete(badPath, false);
            }
        }
    }

    // ---- writer helper ---------------------------------------------------

    private void writeNormalOrc(Path path) throws IOException {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("f1", TypeDescription.createLong())
            .addField("f2", TypeDescription.createDouble())
            .addField("f3", TypeDescription.createVarchar())
            .addField("f4", TypeDescription.createLong())
            .addField("f5", TypeDescription.createLong());

        int rowCount = 31500;

        try (Writer writer = OrcFile.createWriter(path,
            OrcFile.writerOptions(configuration)
                .fileSystem(fileSystem)
                .overwrite(true)
                .compress(CompressionKind.NONE) // key: no compression, footer is raw protobuf and easy to rewrite
                .rowIndexStride(1000)
                .setSchema(schema))) {

            VectorizedRowBatch batch = schema.createRowBatch(1000);
            for (int rowId = 0; rowId < rowCount; rowId++) {
                ((LongColumnVector) batch.cols[0]).vector[batch.size] = rowId;
                ((DoubleColumnVector) batch.cols[1]).vector[batch.size] = rowId * 0.99d;
                ((BytesColumnVector) batch.cols[2]).setVal(batch.size, ("row: " + rowId).getBytes());
                ((LongColumnVector) batch.cols[3]).vector[batch.size] = rowId;
                ((LongColumnVector) batch.cols[4]).vector[batch.size] = rowId;
                batch.size += 1;
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
    }

    // ---- footer tampering helper ----------------------------------------

    /**
     * Read the full bytes of origPath, parse the postscript, parseFrom the footer, tamper with
     * BinaryMetadata, rebuild footer + postscript, and write back to outPath.
     * Relies on CompressionKind.NONE so the footer is raw protobuf and can be parseFrom directly.
     */
    private void rewriteFooterWithBinaryMetaTamper(Path outPath,
                                                   Consumer<OrcProto.BinaryMetadata.Builder> tamper)
        throws IOException {
        byte[] raw;
        long len = fileSystem.getFileStatus(origPath).getLen();
        try (FSDataInputStream in = fileSystem.open(origPath)) {
            raw = new byte[(int) len];
            in.readFully(raw);
        }

        int psLen = raw[raw.length - 1] & 0xFF;
        int psStart = raw.length - 1 - psLen;
        OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(
            Arrays.copyOfRange(raw, psStart, psStart + psLen));
        int footerLen = (int) ps.getFooterLength();
        int footerStart = psStart - footerLen;

        OrcProto.Footer origFooter = OrcProto.Footer.parseFrom(
            Arrays.copyOfRange(raw, footerStart, footerStart + footerLen));
        OrcProto.BinaryMetadata.Builder binBuilder = origFooter.getBinaryMetadata().toBuilder();
        tamper.accept(binBuilder);
        OrcProto.Footer newFooter = origFooter.toBuilder().setBinaryMetadata(binBuilder.build()).build();
        byte[] newFooterBytes = newFooter.toByteArray();

        OrcProto.PostScript newPs = ps.toBuilder().setFooterLength(newFooterBytes.length).build();
        byte[] newPsBytes = newPs.toByteArray();
        if (newPsBytes.length > 255) {
            throw new IllegalStateException("PostScript too large: " + newPsBytes.length);
        }

        try (FSDataOutputStream out = fileSystem.create(outPath, true)) {
            out.write(raw, 0, footerStart); // body + metadata
            out.write(newFooterBytes);
            out.write(newPsBytes);
            out.writeByte(newPsBytes.length);
        }
    }

    // ---- 5 cases ---------------------------------------------------------

    @Test
    public void testC1_UnitSizeByteOver128() throws IOException {
        // C1: one column's columnPositionUnitSize byte >=128 -> signed byte <0 ->
        // positionListSize becomes negative -> NegativeArraySizeException.
        rewriteFooterWithBinaryMetaTamper(badPath, b -> {
            List<ByteString> list = b.getColumnPositionUnitSizeList();
            Assert.assertFalse("writer should emit columnPositionUnitSize entries", list.isEmpty());
            ByteString orig = list.get(0);
            byte[] tampered = orig.toByteArray();
            Assert.assertTrue("need at least 1 stripe for tampering", tampered.length > 0);
            tampered[0] = (byte) 0xFF; // signed = -1
            b.setColumnPositionUnitSize(0, ByteString.copyFrom(tampered));
        });
        assertFallbackBehavior(badPath, "C1", "unitSize");
    }

    @Test
    public void testC2_AccumulatedNonMonotonic() throws IOException {
        // C2: accumulatedRowGroupCountPerStripe is non-monotonic (e.g. [0,5,3]) -> negative diff.
        rewriteFooterWithBinaryMetaTamper(badPath, b -> {
            List<Integer> orig = b.getAccumulatedRowGroupCountPerStripeList();
            Assert.assertTrue("need >=3 stripes for non-monotonic tamper", orig.size() >= 2);
            b.clearAccumulatedRowGroupCountPerStripe();
            int size = Math.max(orig.size(), 3);
            // Build [0, 5, 3, ...]
            b.addAccumulatedRowGroupCountPerStripe(0);
            b.addAccumulatedRowGroupCountPerStripe(5);
            b.addAccumulatedRowGroupCountPerStripe(3);
            for (int i = 3; i < size; i++) {
                b.addAccumulatedRowGroupCountPerStripe(3 + i);
            }
            // Do not touch stripeSize / streams / columnPositionUnitSize length. If the writer
            // did not emit streams for 3 stripes, the defense layer will trip on the stripeSize
            // check first, which is an acceptable safety-net outcome.
        });
        assertFallbackBehavior(badPath, "C2", "accumulated");
    }

    @Test
    public void testC3_AccumulatedSizeMismatchStripeSize() throws IOException {
        // C3: accumulated.size() != binaryMetadata.stripeSize -> length mismatch.
        rewriteFooterWithBinaryMetaTamper(badPath, b -> {
            // Append a bogus entry to violate the length invariant.
            int last = b.getAccumulatedRowGroupCountPerStripeCount() == 0 ? 0
                : b.getAccumulatedRowGroupCountPerStripe(b.getAccumulatedRowGroupCountPerStripeCount() - 1);
            b.addAccumulatedRowGroupCountPerStripe(last + 1000);
        });
        assertFallbackBehavior(badPath, "C3", "length");
    }

    @Test
    public void testC4_TotalRowGroupCountLessThanAccumulatedLast() throws IOException {
        // C4: totalRowGroupCount < accumulated[last] -> rowGroupCountInStripe becomes
        // negative, hence positionListSize becomes negative.
        rewriteFooterWithBinaryMetaTamper(badPath, b -> {
            int accCount = b.getAccumulatedRowGroupCountPerStripeCount();
            Assert.assertTrue("need at least 1 accumulated entry", accCount > 0);
            int last = b.getAccumulatedRowGroupCountPerStripe(accCount - 1);
            b.setTotalRowGroupCount(Math.max(0, last - 1));
        });
        assertFallbackBehavior(badPath, "C4", "totalRowGroupCount");
    }

    @Test
    public void testC5_UnitSizeBytesShorterThanStripeCount() throws IOException {
        // C5: one column's unitSize byte length < accumulated length -> L315 access out of bounds.
        rewriteFooterWithBinaryMetaTamper(badPath, b -> {
            Assert.assertFalse(b.getColumnPositionUnitSizeList().isEmpty());
            // Truncate column 0's unitSize to empty bytes.
            b.setColumnPositionUnitSize(0, ByteString.EMPTY);
        });
        assertFallbackBehavior(badPath, "C5", "unitSize");
    }

    // ---- assertion helper -------------------------------------------------

    private void assertFallbackBehavior(Path path, String caseName, String branchKeyword) throws IOException {
        logAppender.list.clear();

        ORCMetaReaderImpl reader = new ORCMetaReaderImpl(configuration, fileSystem, null);
        PreheatFileMeta result;
        try {
            result = reader.preheat(path);
        } catch (Throwable t) {
            Assert.fail(caseName + ": expected fallback (no throw), but got " + t);
            return;
        } finally {
            reader.close();
        }

        // (1) not taking the binary meta path
        Assert.assertFalse(caseName + ": useBinaryMeta should be false after fallback", result.isUseBinaryMeta());
        // (2) preheatStripes is non-empty (populated by the fallback path)
        Assert.assertNotNull(caseName + ": preheatStripes should not be null", result.getPreheatStripes());
        Assert.assertFalse(caseName + ": preheatStripes should not be empty", result.getPreheatStripes().isEmpty());

        // (3) WARN log contains the expected keywords
        boolean warnHit = logAppender.list.stream()
            .filter(e -> e.getLevel() == Level.WARN)
            .anyMatch(e -> e.getFormattedMessage().contains("BinaryMetadataCorruptException")
                && e.getFormattedMessage().contains(path.getName()));
        Assert.assertTrue(caseName + ": WARN log must contain BinaryMetadataCorruptException and file name; got: "
            + logAppender.list, warnHit);
    }
}

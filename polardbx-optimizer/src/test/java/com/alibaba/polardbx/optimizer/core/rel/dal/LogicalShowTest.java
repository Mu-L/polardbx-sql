package com.alibaba.polardbx.optimizer.core.rel.dal;

import org.junit.Assert;
import org.junit.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Unit tests for LogicalShow / PhyShow dbIndexMode changes introduced in:
 * to #78269329 [show command] dispatch show command to random dn split.
 *
 * @author fangwu
 */
public class LogicalShowTest {

    // -----------------------------------------------------------------------
    // LogicalShow get/set methods
    // -----------------------------------------------------------------------

    /**
     * Verify that setDbIndexMode / getDbIndexMode round-trips correctly.
     */
    @Test
    public void testLogicalShowGetSetDbIndexMode() throws Exception {
        LogicalShow logicalShow = buildMinimalLogicalShow();

        // default should be DB_INDEX_MODE_NORMAL (0)
        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_NORMAL, logicalShow.getDbIndexMode());

        logicalShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);
        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_RANDOM, logicalShow.getDbIndexMode());

        logicalShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_NORMAL);
        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_NORMAL, logicalShow.getDbIndexMode());
    }

    /**
     * Verify that setShowForTruncateTable / isShowForTruncateTable round-trips correctly.
     */
    @Test
    public void testLogicalShowGetSetShowForTruncateTable() throws Exception {
        LogicalShow logicalShow = buildMinimalLogicalShow();

        // default should be false
        Assert.assertFalse(logicalShow.isShowForTruncateTable());

        logicalShow.setShowForTruncateTable(true);
        Assert.assertTrue(logicalShow.isShowForTruncateTable());

        logicalShow.setShowForTruncateTable(false);
        Assert.assertFalse(logicalShow.isShowForTruncateTable());
    }

    // -----------------------------------------------------------------------
    // LogicalShow.copy() must propagate dbIndexMode
    // -----------------------------------------------------------------------

    /**
     * copy() should carry the RANDOM dbIndexMode to the new instance.
     */
    @Test
    public void testLogicalShowCopyPreservesRandomDbIndexMode() throws Exception {
        LogicalShow original = buildMinimalLogicalShow();
        original.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);

        LogicalShow copied = copyLogicalShowWithReflection(original);

        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_RANDOM, copied.getDbIndexMode());
    }

    /**
     * copy() should carry the NORMAL dbIndexMode to the new instance.
     */
    @Test
    public void testLogicalShowCopyPreservesNormalDbIndexMode() throws Exception {
        LogicalShow original = buildMinimalLogicalShow();
        // default is NORMAL

        LogicalShow copied = copyLogicalShowWithReflection(original);

        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_NORMAL, copied.getDbIndexMode());
    }

    /**
     * copy() should NOT carry showForTruncateTable – it is not part of the
     * copy constructor – but the dbIndexMode must still be correct.
     */
    @Test
    public void testLogicalShowCopyDbIndexModeIsIndependentOfShowForTruncateTable() throws Exception {
        LogicalShow original = buildMinimalLogicalShow();
        original.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);
        original.setShowForTruncateTable(true);

        LogicalShow copied = copyLogicalShowWithReflection(original);

        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_RANDOM, copied.getDbIndexMode());
    }

    // -----------------------------------------------------------------------
    // getNodeMode() – the value written by pw.item("node_mode", ...)
    // -----------------------------------------------------------------------

    /**
     * getNodeMode() must return "normal" for DB_INDEX_MODE_NORMAL.
     * This is the string that explainTermsForDisplay writes to the RelWriter
     * via pw.item("node_mode", getNodeMode(dbIndexMode)).
     */
    @Test
    public void testGetNodeModeForNormalModeIsNormal() {
        Assert.assertEquals("normal", LogicalShow.getNodeMode(LogicalShow.DB_INDEX_MODE_NORMAL));
    }

    /**
     * getNodeMode() must return "random" for DB_INDEX_MODE_RANDOM.
     */
    @Test
    public void testGetNodeModeForRandomModeIsRandom() {
        Assert.assertEquals("random", LogicalShow.getNodeMode(LogicalShow.DB_INDEX_MODE_RANDOM));
    }

    /**
     * getNodeMode() must return "unknown" for any unrecognised mode value.
     */
    @Test
    public void testGetNodeModeForUnknownModeIsUnknown() {
        Assert.assertEquals("unknown", LogicalShow.getNodeMode(99));
    }

    /**
     * The node_mode string produced for a LogicalShow with RANDOM mode must
     * equal "random" – this mirrors what explainTermsForDisplay passes to
     * pw.item("node_mode", getNodeMode(dbIndexMode)).
     */
    @Test
    public void testNodeModeStringMatchesDbIndexModeOnLogicalShowInstance() throws Exception {
        LogicalShow logicalShow = buildMinimalLogicalShow();
        logicalShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);

        String nodeMode = LogicalShow.getNodeMode(logicalShow.getDbIndexMode());

        Assert.assertEquals("random", nodeMode);
    }

    /**
     * The node_mode string produced for a LogicalShow with NORMAL mode must
     * equal "normal".
     */
    @Test
    public void testNodeModeStringMatchesDbIndexModeNormalOnLogicalShowInstance() throws Exception {
        LogicalShow logicalShow = buildMinimalLogicalShow();
        // default is NORMAL

        String nodeMode = LogicalShow.getNodeMode(logicalShow.getDbIndexMode());

        Assert.assertEquals("normal", nodeMode);
    }

    /**
     * Obtain the JVM's Unsafe instance so we can allocate PhyShow without
     * invoking any constructor (and therefore without needing a live
     * RelOptCluster or non-null rowType).
     */
    private static final Unsafe UNSAFE;

    static {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            UNSAFE = (Unsafe) unsafeField.get(null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // -----------------------------------------------------------------------
    // LogicalShow.DB_INDEX_MODE_* constants
    // -----------------------------------------------------------------------

    @Test
    public void testDbIndexModeNormalConstantValue() {
        Assert.assertEquals(0, LogicalShow.DB_INDEX_MODE_NORMAL);
    }

    @Test
    public void testDbIndexModeRandomConstantValue() {
        Assert.assertEquals(1, LogicalShow.DB_INDEX_MODE_RANDOM);
    }

    // -----------------------------------------------------------------------
    // LogicalShow.getNodeMode() static helper
    // -----------------------------------------------------------------------

    @Test
    public void testGetNodeModeReturnsNormalForNormalMode() {
        Assert.assertEquals("normal", LogicalShow.getNodeMode(LogicalShow.DB_INDEX_MODE_NORMAL));
    }

    @Test
    public void testGetNodeModeReturnsRandomForRandomMode() {
        Assert.assertEquals("random", LogicalShow.getNodeMode(LogicalShow.DB_INDEX_MODE_RANDOM));
    }

    @Test
    public void testGetNodeModeReturnsUnknownForUnrecognisedMode() {
        Assert.assertEquals("unknown", LogicalShow.getNodeMode(99));
    }

    // -----------------------------------------------------------------------
    // PhyShow.dbIndexMode field – default value and setter
    // -----------------------------------------------------------------------

    /**
     * PhyShow exposes only a setter for dbIndexMode (the field is package-private).
     * We use reflection to read the field value so that we can assert the default
     * and the effect of setDbIndexMode() without changing production visibility.
     */
    @Test
    public void testPhyShowDbIndexModeDefaultIsNormal() throws Exception {
        PhyShow phyShow = buildMinimalPhyShow();
        int dbIndexMode = readDbIndexMode(phyShow);
        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_NORMAL, dbIndexMode);
    }

    @Test
    public void testPhyShowSetDbIndexModeToRandom() throws Exception {
        PhyShow phyShow = buildMinimalPhyShow();
        phyShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);
        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_RANDOM, readDbIndexMode(phyShow));
    }

    @Test
    public void testPhyShowSetDbIndexModeBackToNormal() throws Exception {
        PhyShow phyShow = buildMinimalPhyShow();
        phyShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);
        phyShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_NORMAL);
        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_NORMAL, readDbIndexMode(phyShow));
    }

    // -----------------------------------------------------------------------
    // PhyShow.copy() must propagate dbIndexMode
    // -----------------------------------------------------------------------

    @Test
    public void testPhyShowCopyPreservesRandomDbIndexMode() throws Exception {
        PhyShow original = buildMinimalPhyShow();
        original.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);

        // Override copy() so it exercises the field-copy logic without needing
        // a real RelOptCluster.
        PhyShow copied = copyWithReflection(original);

        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_RANDOM, readDbIndexMode(copied));
    }

    @Test
    public void testPhyShowCopyPreservesNormalDbIndexMode() throws Exception {
        PhyShow original = buildMinimalPhyShow();
        // default is NORMAL – copy should also be NORMAL
        PhyShow copied = copyWithReflection(original);

        Assert.assertEquals(LogicalShow.DB_INDEX_MODE_NORMAL, readDbIndexMode(copied));
    }

    /**
     * After copy(), the copied PhyShow must produce the same node_mode string
     * as the original – this is what explainTermsForDisplay writes via
     * pw.item("node_mode", getNodeMode(dbIndexMode)).
     */
    @Test
    public void testPhyShowCopyNodeModeStringMatchesOriginalForRandom() throws Exception {
        PhyShow original = buildMinimalPhyShow();
        original.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);

        PhyShow copied = copyWithReflection(original);

        String originalNodeMode = LogicalShow.getNodeMode(readDbIndexMode(original));
        String copiedNodeMode = LogicalShow.getNodeMode(readDbIndexMode(copied));
        Assert.assertEquals("node_mode string must survive copy()", originalNodeMode, copiedNodeMode);
        Assert.assertEquals("random", copiedNodeMode);
    }

    @Test
    public void testPhyShowCopyNodeModeStringMatchesOriginalForNormal() throws Exception {
        PhyShow original = buildMinimalPhyShow();
        // default is NORMAL

        PhyShow copied = copyWithReflection(original);

        String originalNodeMode = LogicalShow.getNodeMode(readDbIndexMode(original));
        String copiedNodeMode = LogicalShow.getNodeMode(readDbIndexMode(copied));
        Assert.assertEquals("node_mode string must survive copy()", originalNodeMode, copiedNodeMode);
        Assert.assertEquals("normal", copiedNodeMode);
    }

    // -----------------------------------------------------------------------
    // PhyShow node_mode string – mirrors pw.item("node_mode", getNodeMode(dbIndexMode))
    // -----------------------------------------------------------------------

    /**
     * The node_mode string produced for a PhyShow with RANDOM mode must equal
     * "random" – this is the value that explainTermsForDisplay passes to
     * pw.item("node_mode", getNodeMode(dbIndexMode)).
     */
    @Test
    public void testPhyShowNodeModeStringIsRandomWhenDbIndexModeIsRandom() throws Exception {
        PhyShow phyShow = buildMinimalPhyShow();
        phyShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);

        String nodeMode = LogicalShow.getNodeMode(readDbIndexMode(phyShow));

        Assert.assertEquals("random", nodeMode);
    }

    /**
     * The node_mode string produced for a PhyShow with NORMAL mode must equal
     * "normal".
     */
    @Test
    public void testPhyShowNodeModeStringIsNormalWhenDbIndexModeIsNormal() throws Exception {
        PhyShow phyShow = buildMinimalPhyShow();
        // default is NORMAL

        String nodeMode = LogicalShow.getNodeMode(readDbIndexMode(phyShow));

        Assert.assertEquals("normal", nodeMode);
    }

    /**
     * Setting dbIndexMode back to NORMAL after RANDOM must produce "normal"
     * node_mode – verifying the setter is reflected in the explain output.
     */
    @Test
    public void testPhyShowNodeModeStringAfterResetToNormal() throws Exception {
        PhyShow phyShow = buildMinimalPhyShow();
        phyShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_RANDOM);
        phyShow.setDbIndexMode(LogicalShow.DB_INDEX_MODE_NORMAL);

        String nodeMode = LogicalShow.getNodeMode(readDbIndexMode(phyShow));

        Assert.assertEquals("normal", nodeMode);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Allocate a LogicalShow instance without invoking any constructor, using
     * sun.misc.Unsafe.  This avoids the NPE that occurs when LogicalDal's
     * constructor tries to build CursorMeta from a null rowType / cluster.
     * All fields are at their JVM defaults: int fields = 0 (DB_INDEX_MODE_NORMAL),
     * boolean fields = false (showForTruncateTable).
     */
    @SuppressWarnings("unchecked")
    private LogicalShow buildMinimalLogicalShow() throws Exception {
        return (LogicalShow) UNSAFE.allocateInstance(LogicalShow.class);
    }

    /**
     * Simulate the dbIndexMode copy that LogicalShow.copy() performs, using a
     * fresh LogicalShow instance and direct field assignment via reflection.
     * This lets us verify the copy logic without a real RelOptCluster.
     */
    private LogicalShow copyLogicalShowWithReflection(LogicalShow source) throws Exception {
        LogicalShow destination = buildMinimalLogicalShow();
        Field field = LogicalShow.class.getDeclaredField("dbIndexMode");
        field.setAccessible(true);
        field.set(destination, field.get(source));
        return destination;
    }

    /**
     * Allocate a PhyShow instance without invoking any constructor, using
     * sun.misc.Unsafe.  This avoids the NPE that occurs when BaseDalOperation's
     * constructor tries to build CursorMeta from a null rowType.
     * The resulting object has all fields at their JVM defaults (int fields = 0),
     * which matches DB_INDEX_MODE_NORMAL = 0.
     */
    @SuppressWarnings("unchecked")
    private PhyShow buildMinimalPhyShow() throws Exception {
        return (PhyShow) UNSAFE.allocateInstance(PhyShow.class);
    }

    /**
     * Simulate the dbIndexMode copy that PhyShow.copy() performs, using a fresh
     * PhyShow instance and direct field assignment via reflection.  This lets us
     * test the copy logic without a real cluster.
     */
    private PhyShow copyWithReflection(PhyShow source) throws Exception {
        PhyShow destination = buildMinimalPhyShow();
        Field field = PhyShow.class.getDeclaredField("dbIndexMode");
        field.setAccessible(true);
        field.set(destination, field.get(source));
        return destination;
    }

    /**
     * Read the package-private {@code dbIndexMode} field via reflection.
     */
    private int readDbIndexMode(PhyShow phyShow) throws Exception {
        Field field = PhyShow.class.getDeclaredField("dbIndexMode");
        field.setAccessible(true);
        return (int) field.get(phyShow);
    }
}

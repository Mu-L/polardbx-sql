package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Chunk#getLoadedBlockMemoryUsage()} and
 * {@link Chunk#getLoadedBlockMemoryUsage(boolean[])}.
 */
public class ChunkLoadedBlockMemoryUsageTest {

    private static final long REGULAR_BLOCK_MEMORY = 1024L;
    private static final long LAZY_BLOCK_LOADED_MEMORY = 2048L;

    private Block createRegularBlock(int positionCount) {
        Block block = mock(Block.class);
        when(block.getPositionCount()).thenReturn(positionCount);
        when(block.getMemoryUsage()).thenReturn(REGULAR_BLOCK_MEMORY);
        return block;
    }

    private LazyBlock createLoadedLazyBlock(int positionCount) {
        LazyBlock lazyBlock = mock(LazyBlock.class, Mockito.withSettings().extraInterfaces(Block.class));
        Block loadedBlock = mock(Block.class);
        when(lazyBlock.getPositionCount()).thenReturn(positionCount);
        when(lazyBlock.getLoaded()).thenReturn(loadedBlock);
        when(loadedBlock.getMemoryUsage()).thenReturn(LAZY_BLOCK_LOADED_MEMORY);
        return lazyBlock;
    }

    private LazyBlock createUnloadedLazyBlock(int positionCount) {
        LazyBlock lazyBlock = mock(LazyBlock.class, Mockito.withSettings().extraInterfaces(Block.class));
        when(lazyBlock.getPositionCount()).thenReturn(positionCount);
        when(lazyBlock.getLoaded()).thenReturn(null);
        return lazyBlock;
    }

    // ========== Tests for getLoadedBlockMemoryUsage() ==========

    @Test
    public void testNoParamWithAllRegularBlocks() {
        Block block1 = createRegularBlock(10);
        Block block2 = createRegularBlock(10);
        Chunk chunk = new Chunk(10, block1, block2);

        long memoryUsage = chunk.getLoadedBlockMemoryUsage();

        // INSTANCE_SIZE + 2 * REGULAR_BLOCK_MEMORY
        Assert.assertTrue(memoryUsage > 0);
        Assert.assertTrue(memoryUsage >= 2 * REGULAR_BLOCK_MEMORY);
    }

    @Test
    public void testNoParamWithLoadedLazyBlock() {
        LazyBlock lazyBlock = createLoadedLazyBlock(10);
        Chunk chunk = new Chunk(10, (Block) lazyBlock);

        long memoryUsage = chunk.getLoadedBlockMemoryUsage();

        // INSTANCE_SIZE + LAZY_BLOCK_LOADED_MEMORY
        Assert.assertTrue(memoryUsage >= LAZY_BLOCK_LOADED_MEMORY);
    }

    @Test
    public void testNoParamWithUnloadedLazyBlock() {
        LazyBlock unloadedLazyBlock = createUnloadedLazyBlock(10);
        Chunk chunk = new Chunk(10, (Block) unloadedLazyBlock);

        long memoryUsage = chunk.getLoadedBlockMemoryUsage();

        // Unloaded lazy block should not contribute to memory usage beyond INSTANCE_SIZE
        Assert.assertTrue(memoryUsage > 0);
        Assert.assertTrue(memoryUsage < REGULAR_BLOCK_MEMORY);
    }

    @Test
    public void testNoParamWithMixedBlocks() {
        Block regularBlock = createRegularBlock(10);
        LazyBlock loadedLazyBlock = createLoadedLazyBlock(10);
        LazyBlock unloadedLazyBlock = createUnloadedLazyBlock(10);

        Chunk chunk = new Chunk(10, regularBlock, (Block) loadedLazyBlock, (Block) unloadedLazyBlock);

        long memoryUsage = chunk.getLoadedBlockMemoryUsage();

        // Should include regular block + loaded lazy block, but not unloaded lazy block
        Assert.assertTrue(memoryUsage >= REGULAR_BLOCK_MEMORY + LAZY_BLOCK_LOADED_MEMORY);
    }

    @Test
    public void testNoParamWithSelection() {
        Block block = createRegularBlock(10);
        int[] selection = new int[] {0, 2, 4};
        Chunk chunk = new Chunk(selection, new Block[] {block});

        long memoryUsage = chunk.getLoadedBlockMemoryUsage();

        // Should include selection array memory + block memory
        long memoryWithoutSelection = new Chunk(10, block).getLoadedBlockMemoryUsage();
        Assert.assertTrue(memoryUsage > memoryWithoutSelection);
    }

    @Test
    public void testNoParamWithNullSelection() {
        Block block = createRegularBlock(10);
        Chunk chunk = new Chunk(10, block);

        long memoryUsage = chunk.getLoadedBlockMemoryUsage();

        // No selection, just INSTANCE_SIZE + block memory
        Assert.assertTrue(memoryUsage >= REGULAR_BLOCK_MEMORY);
    }

    // ========== Tests for getLoadedBlockMemoryUsage(boolean[]) ==========

    @Test
    public void testWithBitmapNullSkipsAllBlocks() {
        Block block1 = createRegularBlock(10);
        Block block2 = createRegularBlock(10);
        Chunk chunk = new Chunk(10, block1, block2);

        long memoryUsage = chunk.getLoadedBlockMemoryUsage((boolean[]) null);

        // bitmap is null, all blocks should be skipped
        // Only INSTANCE_SIZE should be counted
        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory, memoryUsage);
    }

    @Test
    public void testWithBitmapSelectsSpecificBlocks() {
        Block block1 = createRegularBlock(10);
        Block block2 = createRegularBlock(10);
        Block block3 = createRegularBlock(10);
        Chunk chunk = new Chunk(10, block1, block2, block3);

        boolean[] bitmap = new boolean[] {true, false, true};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        // Only block1 and block3 should be counted
        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory + 2 * REGULAR_BLOCK_MEMORY, memoryUsage);
    }

    @Test
    public void testWithBitmapShorterThanBlocksLength() {
        Block block1 = createRegularBlock(10);
        Block block2 = createRegularBlock(10);
        Block block3 = createRegularBlock(10);
        Chunk chunk = new Chunk(10, block1, block2, block3);

        // bitmap only covers first 2 blocks, block3 (index=2) should be skipped
        boolean[] bitmap = new boolean[] {true, true};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory + 2 * REGULAR_BLOCK_MEMORY, memoryUsage);
    }

    @Test
    public void testWithBitmapAllFalse() {
        Block block1 = createRegularBlock(10);
        Block block2 = createRegularBlock(10);
        Chunk chunk = new Chunk(10, block1, block2);

        boolean[] bitmap = new boolean[] {false, false};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory, memoryUsage);
    }

    @Test
    public void testWithBitmapAllTrue() {
        Block block1 = createRegularBlock(10);
        Block block2 = createRegularBlock(10);
        Chunk chunk = new Chunk(10, block1, block2);

        boolean[] bitmap = new boolean[] {true, true};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory + 2 * REGULAR_BLOCK_MEMORY, memoryUsage);
    }

    @Test
    public void testWithBitmapAndLoadedLazyBlock() {
        LazyBlock loadedLazyBlock = createLoadedLazyBlock(10);
        Block regularBlock = createRegularBlock(10);
        Chunk chunk = new Chunk(10, (Block) loadedLazyBlock, regularBlock);

        boolean[] bitmap = new boolean[] {true, true};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory + LAZY_BLOCK_LOADED_MEMORY + REGULAR_BLOCK_MEMORY, memoryUsage);
    }

    @Test
    public void testWithBitmapAndUnloadedLazyBlock() {
        LazyBlock unloadedLazyBlock = createUnloadedLazyBlock(10);
        Block regularBlock = createRegularBlock(10);
        Chunk chunk = new Chunk(10, (Block) unloadedLazyBlock, regularBlock);

        boolean[] bitmap = new boolean[] {true, true};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        // Unloaded lazy block should not contribute
        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory + REGULAR_BLOCK_MEMORY, memoryUsage);
    }

    @Test
    public void testWithBitmapAndMixedBlocksPartialSelect() {
        Block regularBlock = createRegularBlock(10);
        LazyBlock loadedLazyBlock = createLoadedLazyBlock(10);
        LazyBlock unloadedLazyBlock = createUnloadedLazyBlock(10);

        Chunk chunk = new Chunk(10, regularBlock, (Block) loadedLazyBlock, (Block) unloadedLazyBlock);

        // Only select the loaded lazy block (index=1)
        boolean[] bitmap = new boolean[] {false, true, false};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory + LAZY_BLOCK_LOADED_MEMORY, memoryUsage);
    }

    @Test
    public void testWithBitmapAndSelection() {
        Block block = createRegularBlock(10);
        int[] selection = new int[] {0, 2, 4};
        Chunk chunk = new Chunk(selection, new Block[] {block});

        boolean[] bitmap = new boolean[] {true};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        // Should include selection array memory + block memory
        Assert.assertTrue(memoryUsage > Chunk.INSTANCE_SIZE + REGULAR_BLOCK_MEMORY);
    }

    @Test
    public void testWithEmptyBitmap() {
        Block block1 = createRegularBlock(10);
        Chunk chunk = new Chunk(10, block1);

        // Empty bitmap means all indices >= bitmap.length, so all blocks skipped
        boolean[] bitmap = new boolean[] {};
        long memoryUsage = chunk.getLoadedBlockMemoryUsage(bitmap);

        long baseMemory = Chunk.INSTANCE_SIZE;
        Assert.assertEquals(baseMemory, memoryUsage);
    }
}

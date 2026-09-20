package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.LongUtil;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.AdaptiveMemoryPool;
import com.alibaba.polardbx.optimizer.memory.GlobalMemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.server.response.FetchCnMemoryPoolSyncAction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyLong;

public class FetchCnMemoryPoolSyncActionTest {

    private MockedStatic<TddlNode> tddlNodeMockedStatic;
    private MockedStatic<MemoryManager> memoryManagerMockedStatic;
    private MockedStatic<LongUtil> longUtilMockedStatic;

    @Before
    public void setup() {
        // Mock TddlNode
        tddlNodeMockedStatic = Mockito.mockStatic(TddlNode.class);
        tddlNodeMockedStatic.when(TddlNode::getHost).thenReturn("localhost");
        tddlNodeMockedStatic.when(TddlNode::getPort).thenReturn(3306);

        // Mock MemoryManager
        memoryManagerMockedStatic = Mockito.mockStatic(MemoryManager.class);

        // Mock LongUtil for bytes conversion
        longUtilMockedStatic = Mockito.mockStatic(LongUtil.class);
        longUtilMockedStatic.when(() -> LongUtil.toBytes(anyLong())).thenAnswer(invocation -> {
            long bytes = invocation.getArgument(0);
            return bytes; // For simplicity, just return the same value in tests
        });
    }

    @Test
    public void testSyncWithSingleMemoryPool() {
        // Setup memory pool hierarchy
        GlobalMemoryPool globalPool = Mockito.mock(GlobalMemoryPool.class);
        Mockito.when(globalPool.getFullName()).thenReturn("global");
        Mockito.when(globalPool.getMemoryUsage()).thenReturn(1000L);
        Mockito.when(globalPool.getMaxLimit()).thenReturn(5000L);
        Mockito.when(globalPool.getChildren()).thenReturn(new HashMap<>());

        memoryManagerMockedStatic.when(MemoryManager::getInstance).thenReturn(Mockito.mock(MemoryManager.class));
        Mockito.when(MemoryManager.getInstance().getGlobalMemoryPool()).thenReturn(globalPool);

        // Execute the action
        FetchCnMemoryPoolSyncAction action = new FetchCnMemoryPoolSyncAction();
        ResultCursor cursor = action.sync();

        // Verify results
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals("localhost:3306", row.getString(0));
        Assert.assertEquals("global", row.getString(1));
        Assert.assertEquals(1000L, (long) row.getLong(2));
        Assert.assertEquals(5000L, (long) row.getLong(3));
        Assert.assertEquals("", row.getString(4));

        // Verify no more rows
        Assert.assertNull(cursor.next());
    }

    @Test
    public void testSyncWithNestedMemoryPools() {
        // Setup memory pool hierarchy with nested pools
        GlobalMemoryPool globalPool = Mockito.mock(GlobalMemoryPool.class);
        MemoryPool childPool1 = Mockito.mock(MemoryPool.class);
        MemoryPool childPool2 = Mockito.mock(AdaptiveMemoryPool.class);

        // Configure global pool
        Mockito.when(globalPool.getFullName()).thenReturn("global");
        Mockito.when(globalPool.getMemoryUsage()).thenReturn(2000L);
        Mockito.when(globalPool.getMaxLimit()).thenReturn(10000L);

        // Configure child pool 1
        Mockito.when(childPool1.getFullName()).thenReturn("global.child1");
        Mockito.when(childPool1.getMemoryUsage()).thenReturn(500L);
        Mockito.when(childPool1.getMaxLimit()).thenReturn(2000L);
        Mockito.when(childPool1.getChildren()).thenReturn(new HashMap<>());

        // Configure child pool 2 (adaptive)
        Mockito.when(childPool2.getFullName()).thenReturn("global.child2");
        Mockito.when(childPool2.getMemoryUsage()).thenReturn(800L);
        Mockito.when(childPool2.getMaxLimit()).thenReturn(MemorySetting.UNLIMITED_SIZE);
        Mockito.when(((AdaptiveMemoryPool) childPool2).getMinLimit()).thenReturn(100L);
        Mockito.when(childPool2.getChildren()).thenReturn(new HashMap<>());

        // Set up hierarchy
        Map<String, MemoryPool> children = new HashMap<>();
        children.put("child1", childPool1);
        children.put("child2", childPool2);
        Mockito.when(globalPool.getChildren()).thenReturn(children);

        memoryManagerMockedStatic.when(MemoryManager::getInstance).thenReturn(Mockito.mock(MemoryManager.class));
        Mockito.when(MemoryManager.getInstance().getGlobalMemoryPool()).thenReturn(globalPool);

        // Execute the action
        FetchCnMemoryPoolSyncAction action = new FetchCnMemoryPoolSyncAction();
        ResultCursor cursor = action.sync();

        // Verify results - should get 3 rows (global + 2 children)
        List<Row> rows = new ArrayList<>();
        Row row;
        while ((row = cursor.next()) != null) {
            rows.add(row);
        }

        Assert.assertEquals(3, rows.size());

        // Verify adaptive pool has proper info
        boolean foundAdaptive = false;
        for (Row r : rows) {
            if (r.getString(1).equals("global.child2")) {
                Assert.assertEquals(-1L, (long) r.getLong(3)); // Unlimited
                Assert.assertTrue(r.getString(4).contains("lowWater=100"));
                Assert.assertTrue(r.getString(4).contains("highWater"));
                foundAdaptive = true;
            }
        }
        Assert.assertTrue(foundAdaptive);
    }

    @Test
    public void testSyncWithUnlimitedPool() {
        // Setup memory pool with unlimited size
        GlobalMemoryPool pool = Mockito.mock(GlobalMemoryPool.class);
        Mockito.when(pool.getFullName()).thenReturn("unlimited");
        Mockito.when(pool.getMemoryUsage()).thenReturn(3000L);
        Mockito.when(pool.getMaxLimit()).thenReturn(MemorySetting.UNLIMITED_SIZE);
        Mockito.when(pool.getChildren()).thenReturn(new HashMap<>());

        memoryManagerMockedStatic.when(MemoryManager::getInstance).thenReturn(Mockito.mock(MemoryManager.class));
        Mockito.when(MemoryManager.getInstance().getGlobalMemoryPool()).thenReturn(pool);

        // Execute the action
        FetchCnMemoryPoolSyncAction action = new FetchCnMemoryPoolSyncAction();
        ResultCursor cursor = action.sync();

        // Verify results
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(-1L, (long) row.getLong(3)); // Unlimited should be -1
    }

    @After
    public void tearDown() {
        if (tddlNodeMockedStatic != null) {
            tddlNodeMockedStatic.close();
        }
        if (memoryManagerMockedStatic != null) {
            memoryManagerMockedStatic.close();
        }
        if (longUtilMockedStatic != null) {
            longUtilMockedStatic.close();
        }
    }
}
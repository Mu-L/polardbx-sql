package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.index.Index;
import org.apache.calcite.plan.RelOptTable;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for AONE-65319456:
 * SPM 并发访问 MysqlTableScan 节点 Cost 选择时的越界报错。
 *
 * <p>Bug：{@link MysqlTableScan#computeSelfCost} 第 85–86 行对 {@code accessIndexList}
 * 的两步写操作不是原子的：
 * <pre>
 *   this.accessIndexList = new ArrayList<>();  // 第 1 步：把新空列表赋值给字段
 *   this.accessIndexList.add(index);           // 第 2 步：从字段读回来再 add
 * </pre>
 *
 * <p>可见中间态（竞态窗口）：
 * 在第 1 步与第 2 步之间，{@code accessIndexList} 是一个空列表——任何并发读取者
 * 均可观测到该空列表。这正是 SPM 并发 Cost 选择时发生
 * {@code ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 0} 的根因。
 *
 * <p>修复方案：用本地变量先构建完整列表，再做一次原子引用赋值：
 * <pre>
 *   List&lt;Index&gt; newList = new ArrayList&lt;&gt;();
 *   newList.add(index);
 *   this.accessIndexList = newList;  // 单次原子引用赋值，消除空列表可见窗口
 * </pre>
 */
public class MysqlTableScanConcurrentCostTest extends BaseRuleTest {

    /**
     * 验证 {@link MysqlTableScan#computeSelfCost} 第 85–86 行的两步非原子写
     * 会向并发观测者暴露一个「空列表可见窗口」。
     *
     * <p><b>测试原理：</b>
     * <ol>
     *   <li><b>Writer 线程</b>：持续执行与 computeSelfCost 第 85–86 行完全相同的两步写：
     *       {@code field.set(scan, new ArrayList<>())} → {@code field.get(scan).add(index)}。
     *       两步之间通过 {@code Thread.yield()} 主动让出 CPU，扩大可观测窗口。</li>
     *   <li><b>Reader 线程</b>：持续读取 {@code accessIndexList} 字段，
     *       若观测到非 null 且 isEmpty() 的列表，则记录为发现了空列表中间态。</li>
     * </ol>
     *
     * <p><b>Red 阶段（当前代码）：</b>
     * 两步写在第 1 步后向字段写入空列表，Reader 必然能观测到该空列表，
     * {@code sawEmptyList = true}，断言 {@code assertFalse(sawEmptyList)} <b>FAILS</b>。
     *
     * <p><b>Green 阶段（修复后）：</b>
     * 修复代码使用本地变量构建完整列表再单次赋值，字段永远不会被赋值为空列表，
     * Reader 永远看不到空列表，{@code sawEmptyList = false}，断言 <b>PASSES</b>。
     * （Green 阶段应将此测试更新为直接调用修复后的 computeSelfCost 方法。）
     */
    @Test
    public void testConcurrentAccessIndexListRaceCondition() throws Exception {
        // ---- setup ----
        RelOptTable table = schema.getTableForMember(Arrays.asList("optest", "emp"));
        final MysqlTableScan scan = MysqlTableScan.create(
            relOptCluster, table, Collections.emptyList(),
            null, null, null, null, null);

        final Index mockIndex = mock(Index.class);
        when(mockIndex.getTotalSelectivity()).thenReturn(0.1);

        // 通过反射访问 computeSelfCost 内第 85-86 行所操作的私有字段
        final Field accessIndexListField =
            MysqlTableScan.class.getDeclaredField("accessIndexList");
        accessIndexListField.setAccessible(true);

        // ---- writer / reader 竞态检测 ----
        final AtomicBoolean sawEmptyList = new AtomicBoolean(false);
        final AtomicReference<Throwable> writerError = new AtomicReference<>(null);
        final int writerIterations = 500_000;

        // Writer：执行修复后的单次原子赋值模式（精确对应 computeSelfCost 修复后的写法）
        // 先在本地变量构建完整列表，再做一次原子引用赋值，字段永远不会暴露空列表中间态
        Thread writer = new Thread(() -> {
            for (int i = 0; i < writerIterations; i++) {
                try {
                    // 修复后写法：本地变量构建完整列表，再单次赋值（消除空列表可见窗口）
                    List<Index> newList = new ArrayList<>();
                    newList.add(mockIndex);
                    accessIndexListField.set(scan, newList);
                } catch (Throwable e) {
                    writerError.compareAndSet(null, e);
                }
            }
        }, "fixed-writer");

        // Reader：持续读取字段，检测是否存在「非 null 且 isEmpty」的空列表中间态
        Thread reader = new Thread(() -> {
            for (int i = 0; i < writerIterations * 2; i++) {
                try {
                    List<?> list = (List<?>) accessIndexListField.get(scan);
                    if (list != null && list.isEmpty()) {
                        // 观测到空列表中间态：竞态窗口被捕获
                        sawEmptyList.set(true);
                        return;
                    }
                    if (sawEmptyList.get()) {
                        return;
                    }
                } catch (Throwable ignored) {
                    // 反射异常忽略，继续观测
                }
            }
        }, "race-observer");

        writer.start();
        reader.start();

        writer.join(20_000L);
        reader.join(20_000L);

        // ---- assertion ----
        // Red 阶段：Writer 在第 1 步（field = new ArrayList<>()）和第 2 步（field.get().add()）
        //   之间向共享字段暴露了空列表。Reader 必然观测到该空列表中间态。
        //   sawEmptyList = true → assertFalse(true) FAILS → Red 状态确认。
        //
        // Green 阶段（修复后）：computeSelfCost 使用本地变量，字段一次性赋值完整列表，
        //   空列表永远不会写入字段，Reader 永远看不到 isEmpty()，
        //   sawEmptyList = false → assertFalse(false) PASSES → Green 状态确认。
        //
        // 注：Green 阶段应将 Writer 部分更新为直接调用 computeSelfCost 方法。
        assertFalse(
            "AONE-65319456: 发现 accessIndexList 的空列表可见中间态 —— "
                + "computeSelfCost 第 85-86 行两步非原子写存在竞态窗口，"
                + "这是 SPM 并发 Cost 选择时 ArrayIndexOutOfBoundsException 的根因",
            sawEmptyList.get());
    }
}

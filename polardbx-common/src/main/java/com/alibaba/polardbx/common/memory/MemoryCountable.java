package com.alibaba.polardbx.common.memory;

import com.google.common.base.Preconditions;
import org.openjdk.jol.info.GraphLayout;

import java.lang.ref.WeakReference;
import java.text.MessageFormat;

public interface MemoryCountable {
    long getMemoryUsage();

    default long getMemoryUsage(ConditionalMemoryType conditionalMemoryType) {
        return getMemoryUsage();
    }

    @SuppressWarnings("unused")
    default boolean isMemoryCountable() {
        return true;
    }

    static double getDeviation(MemoryCountable countable) {
        GraphLayout layout = GraphLayout.parseInstance(countable);
        MemoryUsageReport reportWithoutAnnotation =
            FastMemoryCounter.parseInstance(countable, FastMemoryCounter.DEFAULT_MAX_DEPTH, false, true, false);
        MemoryUsageReport report = FastMemoryCounter.parseInstance(countable);
        long estimatedSize = countable.getMemoryUsage();

        Preconditions.checkArgument(layout.totalSize() == reportWithoutAnnotation.getTotalSize(),
            MessageFormat.format("GraphLayout = {0}, but FastMemoryCounter = {1}", layout.totalSize(),
                reportWithoutAnnotation.getTotalSize()));
        long expected = report.getTotalSize();

        double deviation = ((double) (estimatedSize - expected) / expected);
        return deviation;
    }

    static void checkDeviation(MemoryCountable countable, double expectedDeviation) {
        checkDeviation(countable, expectedDeviation, false);
    }

    static void checkDeviation(MemoryCountable countable, double expectedDeviation, boolean printVerbose) {
        // statistics 1: GraphLayout memory dump.
//        GraphLayout layout = GraphLayout.parseInstance(countable);

//        // statistics 2: TreeStructGraphLayout memory dump. (for test)
//        TreeStructGraphLayout treeLayout = TreeStructGraphLayout.parseInstance(countable);
//
//        // statistics 3: FastMemoryCounter memory dump without any config;
//        MemoryUsageReport reportWithoutAnnotation =
//            FastMemoryCounter.parseInstance(countable, FastMemoryCounter.DEFAULT_MAX_DEPTH, false, true, true);

//        Preconditions.checkArgument(layout.totalSize() == treeLayout.totalSize(),
//            MessageFormat.format("GraphLayout = {0}, but TreeStructGraphLayout = {1}", layout.totalSize(),
//                treeLayout.totalSize()));

//        Preconditions.checkArgument(layout.totalSize() == reportWithoutAnnotation.getTotalSize(),
//            MessageFormat.format("GraphLayout = {0}, but FastMemoryCounter = {1}, tree struct =\n{2}, \n\ncounter struct =\n{3}\n\n", layout.totalSize(),
//                reportWithoutAnnotation.getTotalSize(), treeLayout.printTree(), reportWithoutAnnotation.getMemoryUsageTree()));

        // statistics 3: FastMemoryCounter memory dump with fast config;
        MemoryUsageReport report =
            FastMemoryCounter.parseInstance(countable, FastMemoryCounter.DEFAULT_MAX_DEPTH, true, true, true);
        long estimatedSize = countable.getMemoryUsage();

        long expected = report.getTotalSize();

        double deviation = expected != 0 ? ((double) (estimatedSize - expected) / expected) : 0;

        Preconditions.checkArgument(Math.abs(deviation) <= expectedDeviation,
            MessageFormat.format(
                "memory estimated = {0}, deviation = {1}, but memory report = {2}, expected deviation = {3}, tree = \n{4}",
                estimatedSize, deviation, expected, expectedDeviation, report.getMemoryUsageTree()));

        if (printVerbose) {
            // System.out.println("Without annotation, layout = " + layout.totalSize());
            System.out.println("Deviation : " + deviation);
            System.out.println(report.getMemoryUsageTree());
        }
    }

    static void checkDeviation(MemoryCountable countable, double expectedDeviation, boolean printVerbose,
                               ConditionalMemoryType conditionalMemoryType) {
        // statistics 1: GraphLayout memory dump.
        // GraphLayout layout = GraphLayout.parseInstance(countable);

//        // statistics 2: TreeStructGraphLayout memory dump. (for test)
//        TreeStructGraphLayout treeLayout = TreeStructGraphLayout.parseInstance(countable);
//
//        // statistics 3: FastMemoryCounter memory dump without any config;
//        MemoryUsageReport reportWithoutAnnotation =
//            FastMemoryCounter.parseInstance(
//                countable, FastMemoryCounter.DEFAULT_MAX_DEPTH, false, true, true, conditionalMemoryType);

//        Preconditions.checkArgument(layout.totalSize() == treeLayout.totalSize(),
//            MessageFormat.format("GraphLayout = {0}, but TreeStructGraphLayout = {1}", layout.totalSize(),
//                treeLayout.totalSize()));

//        Preconditions.checkArgument(layout.totalSize() == reportWithoutAnnotation.getTotalSize(),
//            MessageFormat.format("GraphLayout = {0}, but FastMemoryCounter = {1}, tree struct =\n{2}, \n\ncounter struct =\n{3}\n\n", layout.totalSize(),
//                reportWithoutAnnotation.getTotalSize(), treeLayout.printTree(), reportWithoutAnnotation.getMemoryUsageTree()));

        // statistics 3: FastMemoryCounter memory dump with fast config;
        MemoryUsageReport report =
            FastMemoryCounter.parseInstance(countable, FastMemoryCounter.DEFAULT_MAX_DEPTH, true, true, true,
                conditionalMemoryType);
        long estimatedSize = countable.getMemoryUsage(conditionalMemoryType);

        long expected = report.getTotalSize();

        double deviation = expected == 0d ? 0 : ((double) (estimatedSize - expected) / expected);

        Preconditions.checkArgument(Math.abs(deviation) <= expectedDeviation,
            MessageFormat.format(
                "conditionalMemoryType = {0}, memory estimated = {1}, "
                    + "deviation = {2}, but memory report = {3}, expected deviation = {4}, tree = \n{5}",
                conditionalMemoryType, estimatedSize, deviation, expected, expectedDeviation, report.getMemoryUsageTree()));

        if (printVerbose) {
            System.out.println("conditionalMemoryType = " + conditionalMemoryType);
            // System.out.println("Without annotation, layout = " + layout.totalSize());
            System.out.println("Deviation : " + deviation);
            System.out.println(report.getMemoryUsageTree());
        }
    }
}

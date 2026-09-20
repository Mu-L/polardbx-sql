package com.alibaba.polardbx.executor.operator.vectorized;

import com.alibaba.polardbx.common.memory.GlobalMemoryTrackerManager;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.apache.calcite.sql.SqlKind;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class VecSumAvgTest extends GroupByTestBase {
    @Before
    public void config() {
        checkExecutorMemory = true;
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testSumHighCardinality() {
        // build input values by row count and input types.
        int totalRows = 5500;
        int groupKeyBound = 1024;

        GlobalMemoryTrackerManager globalMemoryTrackerManager =
            MemoryTrackerManager.getGlobalMemoryTrackerManager();

        globalMemoryTrackerManager.resize(1L << 30);

        OperatorMemoryOwnerId operatorMemoryOwnerId = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e6e8db400000")
            .createChild(1, 0)
            .createChild(1)
            .createChild(2, "HashAggExec");

        try {
            MemoryTrackerManager.setCurrentMemoryOwner(operatorMemoryOwnerId);

            doAggTest(
                // input data types
                new DataType[] {DataTypes.IntegerType, new DecimalType(15, 2)},
                // group indexes
                new int[] {0},
                // agg indexes
                new int[][] {{1}},
                // agg function
                new SqlKind[] {SqlKind.SUM},
                totalRows, groupKeyBound);

        } finally {
            MemoryTrackerManager.removeCurrentMemoryOwner();
        }

    }

    @Test
    public void testSumInteger() {
        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 8;

        GlobalMemoryTrackerManager globalMemoryTrackerManager =
            MemoryTrackerManager.getGlobalMemoryTrackerManager();

        globalMemoryTrackerManager.resize(1L << 30);

        OperatorMemoryOwnerId operatorMemoryOwnerId = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e6e8db400000")
            .createChild(1, 0)
            .createChild(1)
            .createChild(2, "HashAggExec");

        try {
            MemoryTrackerManager.setCurrentMemoryOwner(operatorMemoryOwnerId);

            doAggTest(
                // input data types
                new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType},
                // group indexes
                new int[] {0},
                // agg indexes
                new int[][] {{1}},
                // agg function
                new SqlKind[] {SqlKind.SUM},
                totalRows, groupKeyBound);

            doAggTest(
                // input data types
                new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType},
                // group indexes
                new int[] {0, 1},
                // agg indexes
                new int[][] {{2}},
                // agg function
                new SqlKind[] {SqlKind.SUM},
                // reduce cardinality in group-by
                totalRows, groupKeyBound / 2);

            doAggTest(
                // input data types
                new DataType[] {
                    DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType},
                // group indexes
                new int[] {0, 1, 2},
                // agg indexes
                new int[][] {{3}},
                // agg function
                new SqlKind[] {SqlKind.SUM},
                // reduce cardinality in group-by
                totalRows, groupKeyBound / 3);

        } finally {
            MemoryTrackerManager.removeCurrentMemoryOwner();
        }
    }

    @Test
    public void testSumLongIntegerLong() {
        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 8;

        GlobalMemoryTrackerManager globalMemoryTrackerManager =
            MemoryTrackerManager.getGlobalMemoryTrackerManager();

        globalMemoryTrackerManager.resize(1L << 30);

        OperatorMemoryOwnerId operatorMemoryOwnerId = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e6e8db400000")
            .createChild(1, 0)
            .createChild(1)
            .createChild(2, "HashAggExec");

        try {
            MemoryTrackerManager.setCurrentMemoryOwner(operatorMemoryOwnerId);

            doAggTest(
                // input data types
                new DataType[] {DataTypes.LongType, DataTypes.IntegerType, DataTypes.LongType, DataTypes.LongType},
                // group indexes
                new int[] {0, 1, 2},
                // agg indexes
                new int[][] {{3}},
                // agg function
                new SqlKind[] {SqlKind.SUM},
                // reduce cardinality in group-by
                totalRows, groupKeyBound / 3);

        } finally {
            MemoryTrackerManager.removeCurrentMemoryOwner();
        }

    }

    @Test
    public void testSum3() {
        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 8;

        GlobalMemoryTrackerManager globalMemoryTrackerManager =
            MemoryTrackerManager.getGlobalMemoryTrackerManager();

        globalMemoryTrackerManager.resize(1L << 30);

        OperatorMemoryOwnerId operatorMemoryOwnerId = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e6e8db400000")
            .createChild(1, 0)
            .createChild(1)
            .createChild(2, "HashAggExec");

        try {
            MemoryTrackerManager.setCurrentMemoryOwner(operatorMemoryOwnerId);

            doAggTest(
                // input data types
                new DataType[] {DataTypes.IntegerType, new DecimalType(18, 2)},
                // group indexes
                new int[] {0},
                // agg indexes
                new int[][] {{1}},
                // agg function
                new SqlKind[] {SqlKind.SUM},
                // reduce cardinality in group-by
                totalRows, groupKeyBound);

        } finally {
            MemoryTrackerManager.removeCurrentMemoryOwner();
        }

    }

    @Test
    public void testSumIntegerInteger() {
        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 8;

        GlobalMemoryTrackerManager globalMemoryTrackerManager =
            MemoryTrackerManager.getGlobalMemoryTrackerManager();

        globalMemoryTrackerManager.resize(1L << 30);

        OperatorMemoryOwnerId operatorMemoryOwnerId = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e6e8db400000")
            .createChild(1, 0)
            .createChild(1)
            .createChild(2, "HashAggExec");

        try {
            MemoryTrackerManager.setCurrentMemoryOwner(operatorMemoryOwnerId);

            doAggTest(
                // input data types
                new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType, new DecimalType(18, 2)},
                // group indexes
                new int[] {0, 1},
                // agg indexes
                new int[][] {{2}},
                // agg function
                new SqlKind[] {SqlKind.SUM},
                // reduce cardinality in group-by
                totalRows, groupKeyBound);

        } finally {
            MemoryTrackerManager.removeCurrentMemoryOwner();
        }
    }

    @Test
    public void testSumLong() {
        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 8;

        GlobalMemoryTrackerManager globalMemoryTrackerManager =
            MemoryTrackerManager.getGlobalMemoryTrackerManager();

        globalMemoryTrackerManager.resize(1L << 30);

        OperatorMemoryOwnerId operatorMemoryOwnerId = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e6e8db400000")
            .createChild(1, 0)
            .createChild(1)
            .createChild(2, "HashAggExec");
        try {
            MemoryTrackerManager.setCurrentMemoryOwner(operatorMemoryOwnerId);

            doAggTest(
                // input data types
                new DataType[] {DataTypes.LongType, new DecimalType(18, 2)},
                // group indexes
                new int[] {0},
                // agg indexes
                new int[][] {{1}},
                // agg function
                new SqlKind[] {SqlKind.SUM},
                // reduce cardinality in group-by
                totalRows, groupKeyBound);

        } finally {
            MemoryTrackerManager.removeCurrentMemoryOwner();
        }

    }

    @Test
    public void testAvg() {
        // metadata
        final DataType[] inputDataTypes = {DataTypes.IntegerType, DataTypes.IntegerType};
        final int[] groups = {0};
        final int[][] aggregatorIndexes = {{1}};
        final SqlKind[] aggKinds = {SqlKind.AVG};

        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 10;
        doAggTest(inputDataTypes, groups, aggregatorIndexes, aggKinds, totalRows, groupKeyBound);
    }
}

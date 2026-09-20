package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.mpp.execution.EmptyMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalBufferExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalExchangeConsumerFactory;
import com.alibaba.polardbx.executor.mpp.planner.LocalExchange;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class LocalExchangeConsumerFactoryTest extends BaseExecTest {
    private long localBufferSize;
    private Executor notificationExecutor;

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1024);

        // open vectorization implementation o f join probing and rows building.
        connectionMap.put(ConnectionParams.ENABLE_EXCHANGE_PARTITION_OPTIMIZATION.getName(), true);
        connectionMap.put(ConnectionParams.ENABLE_LOCAL_EXCHANGE_BATCH.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));

        localBufferSize = context.getParamManager().getLong(ConnectionParams.MPP_TASK_LOCAL_MAX_BUFFER_SIZE);
        notificationExecutor = Executors.newSingleThreadExecutor();
    }

    @Test
    public void testCreateExecutor() {
        doTest(LocalExchange.LocalExchangeMode.PARTITION);
        doTest(LocalExchange.LocalExchangeMode.CHUNK_PARTITION);
        doTest(LocalExchange.LocalExchangeMode.SINGLE);
        doTest(LocalExchange.LocalExchangeMode.DIRECT);
        doTest(LocalExchange.LocalExchangeMode.RANDOM);
        doTest(LocalExchange.LocalExchangeMode.BORADCAST);
    }

    private void doTest(LocalExchange.LocalExchangeMode mode) {
        // consistent data types during exchange.
        final List<DataType> exchangeDataTypes =
            ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType, DataTypes.IntegerType);

        // parallelism for consumer.
        final int parallelism = 4;

        // which column index to calculate hash.
        final List<Integer> partitionChannels = ImmutableList.of(0);

        // build partition exchanger
        LocalExchange.LocalExchangeMode exchangeMode = mode;

        OutputBufferMemoryManager outputBufferMemoryManager =
            new OutputBufferMemoryManager(localBufferSize, new EmptyMemSystemListener(), notificationExecutor);

        ExecutorFactory parentExecutorFactory =
            new LocalBufferExecutorFactory(outputBufferMemoryManager, exchangeDataTypes, parallelism);

        LocalExchange localExchange = new LocalExchange(exchangeDataTypes, partitionChannels, exchangeMode, true);

        LocalExchangeConsumerFactory factory =
            new LocalExchangeConsumerFactory(parentExecutorFactory, outputBufferMemoryManager, localExchange);

        for (int i = 0; i < parallelism; i++) {
            factory.createExecutor(context, i);
        }
    }

}
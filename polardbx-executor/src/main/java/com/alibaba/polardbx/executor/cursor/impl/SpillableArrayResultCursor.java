package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * 类似{@link ArrayResultCursor}的写入式结果cursor，但缓存的行内存超过阈值后
 * 会通过spill框架写入本地临时文件，读取时从spill文件反序列化，close时删除临时文件。
 * <p>
 * 使用方式：addColumn() -> initMeta() -> addRow()... -> completeWrite() -> next()... -> close()
 *
 * @author lijiu
 */
public class SpillableArrayResultCursor extends ResultCursor {

    private final String tableName;
    private final SpillerFactory spillerFactory;
    /**
     * 内存中缓存行的估算大小超过该值时spill到本地文件，单位字节
     */
    private final long spillMemoryLimit;

    private final List<Row> bufferRows = new LinkedList<>();
    private CursorMeta meta;
    private long bufferedSize = 0;
    private boolean writeCompleted = false;

    private final ExecutionContext context = new ExecutionContext();
    private final int chunkLimit;
    private DataType[] dataTypes;
    private BlockBuilder[] blockBuilders;
    private QuerySpillSpaceMonitor spillMonitor;
    private Spiller spiller;

    private Iterator<Chunk> chunkIterator;
    private Chunk currentChunk;
    private int currentPos;
    private Iterator<Row> memoryIterator;

    public SpillableArrayResultCursor(String tableName, SpillerFactory spillerFactory, long spillMemoryLimit) {
        super(null);
        this.tableName = tableName;
        this.spillerFactory = spillerFactory;
        this.spillMemoryLimit = spillMemoryLimit;
        this.returnColumns = new ArrayList<>();
        this.chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
    }

    public void addColumn(String columnName, DataType type) {
        Field field = new Field(tableName, columnName, type);
        ColumnMeta c = new ColumnMeta(this.tableName, TStringUtil.upperCase(columnName), null, field);
        returnColumns.add(c);
    }

    public void initMeta() {
        this.meta = CursorMeta.build(returnColumns);
        this.dataTypes = new DataType[returnColumns.size()];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = returnColumns.get(i).getDataType();
        }
    }

    public void addRow(Object[] values) {
        Preconditions.checkState(!writeCompleted, "cursor is already completed for write");
        if (meta == null) {
            initMeta();
        }
        long rowSize = estimateRowSize(values);
        bufferRows.add(new ArrayRow(meta, values, rowSize));
        bufferedSize += rowSize;
        if (bufferedSize >= spillMemoryLimit && spillerFactory != null) {
            spillBuffer();
        }
    }

    /**
     * 写入完成，之后只能通过next()读取
     */
    public void completeWrite() {
        if (writeCompleted) {
            return;
        }
        writeCompleted = true;
        if (spiller != null) {
            // 已发生过spill，为保证行的顺序，剩余内存中的行也全部spill
            if (!bufferRows.isEmpty()) {
                spillBuffer();
            }
            chunkIterator = Iterators.concat(spiller.getSpills().iterator());
        } else {
            memoryIterator = bufferRows.iterator();
        }
    }

    @Override
    public Row doNext() {
        if (closed) {
            return null;
        }
        if (!writeCompleted) {
            completeWrite();
        }
        if (chunkIterator != null) {
            while (true) {
                if (currentChunk == null) {
                    if (chunkIterator.hasNext()) {
                        currentChunk = chunkIterator.next();
                    }
                    if (currentChunk == null) {
                        return null;
                    }
                }
                if (currentPos < currentChunk.getPositionCount()) {
                    Row row = currentChunk.rowAt(currentPos++);
                    row.setCursorMeta(meta);
                    return row;
                } else {
                    currentChunk = null;
                    currentPos = 0;
                }
            }
        } else {
            if (memoryIterator != null && memoryIterator.hasNext()) {
                return memoryIterator.next();
            }
            return null;
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        if (exceptions == null) {
            exceptions = new ArrayList<>();
        }
        if (closed) {
            return exceptions;
        }
        closed = true;
        try {
            if (spiller != null) {
                // 删除本地spill临时文件
                spiller.close();
            }
        } catch (Throwable t) {
            exceptions.add(t);
        }
        try {
            if (spillMonitor != null) {
                spillMonitor.close();
            }
        } catch (Throwable t) {
            exceptions.add(t);
        }
        bufferRows.clear();
        return exceptions;
    }

    @Override
    public CursorMeta getCursorMeta() {
        return meta;
    }

    public boolean isSpilled() {
        return spiller != null;
    }

    public String getTableName() {
        return tableName;
    }

    private void spillBuffer() {
        if (spiller == null) {
            createBlockBuilders();
            spillMonitor = new QuerySpillSpaceMonitor(tableName);
            spiller = spillerFactory.create(Lists.newArrayList(dataTypes), spillMonitor, null);
        }
        ListenableFuture<?> spillFuture = spiller.spill(buildChunkIterator(), true);
        // 同步等待spill完成，保证下一次spill前上一次已结束
        ExecUtils.checkException(spillFuture);
        bufferRows.clear();
        bufferedSize = 0;
    }

    private void createBlockBuilders() {
        if (blockBuilders == null) {
            blockBuilders = new BlockBuilder[dataTypes.length];
            for (int i = 0; i < dataTypes.length; i++) {
                blockBuilders[i] = BlockBuilders.create(dataTypes[i], context);
            }
        }
    }

    private Iterator<Chunk> buildChunkIterator() {
        return new Iterator<Chunk>() {
            private final Iterator<Row> iterator = bufferRows.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext() || blockBuilders[0].getPositionCount() > 0;
            }

            @Override
            public Chunk next() {
                while (iterator.hasNext()) {
                    Row currentRow = iterator.next();
                    for (int i = 0; i < currentRow.getColNum(); i++) {
                        blockBuilders[i].writeObject(dataTypes[i].convertFrom(currentRow.getObject(i)));
                    }
                    if (blockBuilders[0].getPositionCount() >= chunkLimit) {
                        return buildChunkAndReset();
                    }
                }
                if (blockBuilders[0].getPositionCount() > 0) {
                    return buildChunkAndReset();
                }
                return null;
            }
        };
    }

    private Chunk buildChunkAndReset() {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    /**
     * 粗略估算一行数据的内存占用，用于spill阈值判断
     */
    private static long estimateRowSize(Object[] values) {
        long size = 48;
        for (Object value : values) {
            if (value == null) {
                size += 8;
            } else if (value instanceof String) {
                size += 40 + 2L * ((String) value).length();
            } else if (value instanceof byte[]) {
                size += 40 + ((byte[]) value).length;
            } else if (value instanceof Number) {
                size += 24;
            } else {
                size += 64;
            }
        }
        return size;
    }
}

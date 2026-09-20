package com.alibaba.polardbx.common.orc;

import org.apache.orc.OrcProto;
import org.apache.orc.impl.MetadataDeserializeUtils;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class FastColumnEncodingIndexImpl implements FastColumnEncodingIndex {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FastColumnEncodingIndexImpl.class).instanceSize();
    private final short[] shortColumnEncodingList;
    private final int[] columnEncodingList;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(columnEncodingList))
            + VMSupport.align((int) sizeOf(shortColumnEncodingList));
    }

    public FastColumnEncodingIndexImpl(short[] shortColumnEncodingList,
                                       int[] columnEncodingList) {
        this.shortColumnEncodingList = shortColumnEncodingList;
        this.columnEncodingList = columnEncodingList;
    }

    public OrcProto.ColumnEncoding getColumnEncoding(int stripeId) {
        if (columnEncodingList != null) {
            OrcProto.ColumnEncoding.Builder columnEncodingBuilder = OrcProto.ColumnEncoding.newBuilder();
            int kindAndBloomEncoding = columnEncodingList[stripeId * 2];
            int dictionarySize = columnEncodingList[stripeId * 2 + 1];

            MetadataDeserializeUtils.deserializeColumnEncodingKindAndBloomEncoding(columnEncodingBuilder,
                kindAndBloomEncoding);
            columnEncodingBuilder.setDictionarySize(dictionarySize);
            OrcProto.ColumnEncoding columnEncoding = columnEncodingBuilder.build();
            return columnEncoding;
        } else {
            short kindAndBloomEncoding = shortColumnEncodingList[stripeId * 2];
            short dictionarySize = shortColumnEncodingList[stripeId * 2 + 1];
            OrcProto.ColumnEncoding.Builder columnEncodingBuilder = OrcProto.ColumnEncoding.newBuilder();
            MetadataDeserializeUtils.deserializeColumnEncodingKindAndBloomEncodingShort(columnEncodingBuilder,
                kindAndBloomEncoding);
            columnEncodingBuilder.setDictionarySize(dictionarySize);
            OrcProto.ColumnEncoding columnEncoding = columnEncodingBuilder.build();
            return columnEncoding;
        }
    }
}

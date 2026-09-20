/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.PositionProviderBuilder;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.ReaderEncryptionVariant;
import org.openjdk.jol.info.ClassLayout;

import java.util.HashMap;
import java.util.Map;

public class PreheatFileMeta implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PreheatFileMeta.class).instanceSize();

    // for old version orc meta
    private Map<Long, PreheatStripeMeta> preheatStripes;

    // for new version orc meta
    private boolean useBinaryMeta;
    // for coordinator
    private FastColumnStatistics[] fastColumnStatisticsArray;
    // for worker
    private FastPositionIndex[] fastPositionIndexArray;

    private FastColumnEncodingIndex[] fastColumnEncodingIndexArray;
    private FastStreamIndex[] fastStreamIndexArray;
    private String[] writerTimeZoneArray;

    // common part
    private OrcTail preheatTail;
    private FileStatus fileStatus;
    private long memorySize;

    public PreheatFileMeta() {
    }

    public int getStripeSize() {
        if (preheatStripes != null) {
            return preheatStripes.size();
        } else {
            return writerTimeZoneArray.length;
        }
    }

    public PositionProviderBuilder getPositionProviderBuilder(long stripeIndex) {
        if (preheatStripes != null) {
            return preheatStripes.get(stripeIndex).getOrcIndex();
        } else {
            return new FastPositionIndexPositionProviderBuilder((int) stripeIndex, fastPositionIndexArray);
        }
    }

    public FastColumnStatistics[] getFastColumnStatisticsArray() {
        return fastColumnStatisticsArray;
    }

    public void setFastColumnStatisticsArray(
        FastColumnStatistics[] fastColumnStatisticsArray) {
        this.fastColumnStatisticsArray = fastColumnStatisticsArray;
    }

    public FastPositionIndex[] getFastPositionIndexArray() {
        return fastPositionIndexArray;
    }

    public void setFastPositionIndexArray(
        FastPositionIndex[] fastPositionIndexArray) {
        this.fastPositionIndexArray = fastPositionIndexArray;
    }

    public void setPreheatStripes(
        Map<Long, PreheatStripeMeta> preheatStripes) {
        this.preheatStripes = preheatStripes;
    }

    public OrcTail getPreheatTail() {
        return preheatTail;
    }

    public Map<Long, PreheatStripeMeta> getPreheatStripes() {
        return preheatStripes;
    }

    public void setPreheatTail(OrcTail preheatTail) {
        this.preheatTail = preheatTail;
    }

    public OrcProto.RowIndex getRowGroupIndex(long stripeIndex, int columnId) {
        PreheatStripeMeta stripeMeta = preheatStripes.get(stripeIndex);
        Preconditions.checkNotNull(stripeMeta);

        OrcIndex orcIndex = stripeMeta.getOrcIndex();
        return orcIndex.getRowGroupIndex()[columnId];
    }

    /**
     * The length of encoding[] array is equal to column count,
     * and the encoding of column that not included is null.
     */
    public OrcProto.ColumnEncoding[] buildEncodings(
        long stripeIndex,
        boolean[] columnInclude) {
        OrcProto.ColumnEncoding[] encodings =
            new OrcProto.ColumnEncoding[columnInclude.length];

        if (useBinaryMeta) {
            for (int c = 0; c < encodings.length; ++c) {
                if (columnInclude == null || columnInclude[c]) {
                    encodings[c] = fastColumnEncodingIndexArray[c].getColumnEncoding((int) stripeIndex);
                }
            }
        } else {
            for (int c = 0; c < encodings.length; ++c) {
                if (columnInclude == null || columnInclude[c]) {
                    encodings[c] = preheatStripes.get(stripeIndex).getStripeFooter().getColumns(c);
                }
            }
        }

        return encodings;
    }

    public OrcProto.ColumnEncoding getColumnEncoding(long stripeIndex, int columnId) {
        if (useBinaryMeta) {
            return fastColumnEncodingIndexArray[columnId].getColumnEncoding((int) stripeIndex);
        } else {
            return preheatStripes.get(stripeIndex).getStripeFooter().getColumns(columnId);
        }
    }

    public OrcProto.ColumnEncoding[] getColumnEncodings(long stripeIndex) {
        if (useBinaryMeta) {
            final int columnCount = fastColumnEncodingIndexArray.length;
            OrcProto.ColumnEncoding[] encodings = new OrcProto.ColumnEncoding[columnCount];
            for (int c = 0; c < columnCount; ++c) {
                encodings[c] = fastColumnEncodingIndexArray[c].getColumnEncoding((int) stripeIndex);
            }
            return encodings;
        } else {
            OrcProto.StripeFooter stripeFooter = preheatStripes.get(stripeIndex).getStripeFooter();
            return stripeFooter.getColumnsList().toArray(new OrcProto.ColumnEncoding[0]);
        }
    }

    public OrcProto.StripeFooter findStripeFooterInNonBinaryMode(long stripeIndex) {
        if (useBinaryMeta) {
            throw new IllegalArgumentException("not support in non-binary mode");
        }
        PreheatStripeMeta stripeMeta = preheatStripes.get(stripeIndex);
        return stripeMeta.getStripeFooter();
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }

    public void setFileStatus(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public FastColumnEncodingIndex[] getFastColumnEncodingIndexArray() {
        return fastColumnEncodingIndexArray;
    }

    public void setFastColumnEncodingIndexArray(FastColumnEncodingIndex[] fastColumnEncodingIndexArray) {
        this.fastColumnEncodingIndexArray = fastColumnEncodingIndexArray;
    }

    public FastStreamIndex[] getFastStreamIndexArray() {
        return fastStreamIndexArray;
    }

    public void setFastStreamIndexArray(FastStreamIndex[] fastStreamIndexArray) {
        this.fastStreamIndexArray = fastStreamIndexArray;
    }

    public String[] getWriterTimeZoneArray() {
        return writerTimeZoneArray;
    }

    public String getWriterTimeZone(int stripeIndex) {
        return writerTimeZoneArray[stripeIndex];
    }

    public void setWriterTimeZoneArray(String[] writerTimeZoneArray) {
        this.writerTimeZoneArray = writerTimeZoneArray;
    }

    public FastStreamIndex getFastStreamIndex(int stripeIndex) {
        return fastStreamIndexArray[stripeIndex];
    }

    public boolean isUseBinaryMeta() {
        return useBinaryMeta;
    }

    public void setUseBinaryMeta(boolean useBinaryMeta) {
        this.useBinaryMeta = useBinaryMeta;
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE;
    }
}

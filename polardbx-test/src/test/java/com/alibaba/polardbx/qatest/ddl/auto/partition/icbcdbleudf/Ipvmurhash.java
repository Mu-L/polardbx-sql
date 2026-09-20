package com.alibaba.polardbx.qatest.ddl.auto.partition.icbcdbleudf;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedParameterizedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DblePartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.RuleAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.DbleAlgorithmInitParams;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.PartitionUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.SplitUtil;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

public class Ipvmurhash extends UserDefinedParameterizedJavaFunction  {
    private DblePartitionAlgorithm routeFunc;
    private Map<String, Object> algorithmParamsMappings = null;

    public Ipvmurhash() {
    }

    public class PartitionByIpvsMurmurHash extends DblePartitionAlgorithm implements RuleAlgorithm {
        private static final long serialVersionUID = -4712399083043025898L;

        protected int[] count;

        protected int[] length;

        protected PartitionUtil partitionUtil;

        public PartitionByIpvsMurmurHash() {
        }

        private int[] toIntArray(String string) {
            String[] strs = SplitUtil.split(string, ',', true);
            int[] ints = new int[strs.length];
            for (int i = 0; i < strs.length; i++)
                ints[i] = Integer.parseInt(strs[i]);
            return ints;
        }

        public void setPartitionCount(String partitionCount) {
            this.count = toIntArray(partitionCount);
        }

        public void setPartitionLength(String partitionLength) {
            this.length = toIntArray(partitionLength);
        }

        public void init() {
            this.partitionUtil = new PartitionUtil(this.count, this.length);
        }

        private Integer calculate(long key) {
            return Integer.valueOf(this.partitionUtil.partition(key));
        }

        public Integer calculate(String columnValue) {
            try {
                MurmurHash murmurHash = new MurmurHash();
                long key = murmurHash.hash(columnValue);
                return calculate(key);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
            }
        }

        public Integer[] calculateRange(String beginValue, String endValue) {
            return new Integer[0];
        }

        public void selfCheck() {}
    }


    public final class MurmurHash {
        public int hash(byte[] data, int seed) {
            return hash(ByteBuffer.wrap(data), seed);
        }

        public int hash(byte[] data, int offset, int length, int seed) {
            return hash(ByteBuffer.wrap(data, offset, length), seed);
        }

        public int hash(ByteBuffer buf, int seed) {
            ByteOrder byteOrder = buf.order();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            int m = 1540483477;
            int r = 24;
            int h = seed ^ buf.remaining();
            while (buf.remaining() >= 4) {
                int k = buf.getInt();
                k *= m;
                k ^= k >>> r;
                k *= m;
                h *= m;
                h ^= k;
            }
            if (buf.remaining() > 0) {
                ByteBuffer finish = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                finish.put(buf).rewind();
                h ^= finish.getInt();
                h *= m;
            }
            h ^= h >>> 13;
            h *= m;
            h ^= h >>> 15;
            buf.order(byteOrder);
            return h;
        }

        public long hash64A(byte[] data, int seed) {
            return hash64A(ByteBuffer.wrap(data), seed);
        }

        public long hash64A(byte[] data, int offset, int length, int seed) {
            return hash64A(ByteBuffer.wrap(data, offset, length), seed);
        }

        public long hash64A(ByteBuffer buf, int seed) {
            ByteOrder byteOrder = buf.order();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            long m = -4132994306676758123L;
            int r = 47;
            long h = seed ^ buf.remaining() * m;
            while (buf.remaining() >= 8) {
                long k = buf.getLong();
                k *= m;
                k ^= k >>> r;
                k *= m;
                h ^= k;
                h *= m;
            }
            if (buf.remaining() > 0) {
                ByteBuffer finish = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                finish.put(buf).rewind();
                h ^= finish.getLong();
                h *= m;
            }
            h ^= h >>> r;
            h *= m;
            h ^= h >>> r;
            buf.order(byteOrder);
            return h;
        }

        public long hash(byte[] key) {
            return hash64A(key, 305441741);
        }

        public long hash(String key) {
            String lkey = key;
            try {
                if (lkey == null)
                    return 0L;
                lkey = lkey.trim();
                if (lkey.isEmpty())
                    return 0L;
                if (isDigit(lkey)) {
                    BigInteger bi = new BigInteger(lkey);
                    lkey = bi.toString();
                }
                return Math.abs(hash(lkey.getBytes("GBK")));
            } catch (UnsupportedEncodingException e) {
                return Math.abs(hash(lkey.getBytes()));
            }
        }

        private boolean isDigit(String strNum) {
            return strNum.matches("[0-9]{1,}");
        }
    }

    @Override
    protected void initFunction(String initParams) {
        algorithmParamsMappings = (Map<String, Object>) DbleAlgorithmInitParams.buildMappingsFromJsonParams(initParams);
        String partitionCount = (String) algorithmParamsMappings.get("partitionCount");
        String partitionLength = (String) algorithmParamsMappings.get("partitionLength");
        String dataNodeCount = (String) algorithmParamsMappings.get("dataNodeCount");
        init(partitionCount, partitionLength, dataNodeCount);
    }

    @Override
    public Object getInitParamsInfo() {
        return algorithmParamsMappings;
    }

    private void init(String partitionCountStr, String partitionLengthStr, String dataNodeCountStr) {
        PartitionByIpvsMurmurHash newPart = new PartitionByIpvsMurmurHash();
        newPart.setPartitionCount(partitionCountStr);
        newPart.setPartitionLength(partitionLengthStr);
        newPart.init();
        checkIfPartCountMatchAlgorithm(newPart, dataNodeCountStr);
        this.routeFunc = newPart;
    }

    protected void checkIfPartCountMatchAlgorithm(DblePartitionAlgorithm initedAlgorithm,
                                                  String dataNodeCountStr) {
        Integer dataNodeCount = null;
        if(dataNodeCountStr != null && dataNodeCountStr.length() > 0) {
            dataNodeCount = Integer.valueOf(dataNodeCountStr);
        }
        if (dataNodeCount != null) {
            int partNumOfAlgorithm = initedAlgorithm.getPartitionNum();
            int dataNodeCountInt = dataNodeCount;
            int suitValue = initedAlgorithm.suitableFor(dataNodeCountInt);
            if (suitValue < 0) {
                String errMsg = String.format(
                    "function partition size : %s, datanode size is %s, please make sure table datanode size = function partition size",
                    partNumOfAlgorithm, dataNodeCount);
                throw new RuntimeException(errMsg);
            } else if (suitValue > 0) {
                //String warnMsg = String.format("function partition size : %s, datanode size is %s, table datanode size > function partition size",partNumOfAlgorithm, dataNodeCount);
                // ignore
            } else {
                // table data node size == rule function partition size
            }
        }
    }

    @Override
    public Object compute(Object[] args) {
        if (!isInited) {
            throw new RuntimeException(String.format("udf function %s has not init yet", getFunctionNames()[0]));
        }
        return  routeFunc.compute(args);
    }

}

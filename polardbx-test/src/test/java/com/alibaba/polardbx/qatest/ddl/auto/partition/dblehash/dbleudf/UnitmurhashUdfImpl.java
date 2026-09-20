/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DblePartitionAlgorithm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class UnitmurhashUdfImpl extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public static class MurmurHash {

        // MurMurHash
        public static final String TABLE_COLUMN_SEPARATOR = ".";

        private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
        private static final Random RANDOM = new Random();
        private static final char[] CHARS = {
            '1', '2', '3', '4', '5', '6', '7',
            '8', '9', '0', 'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p',
            'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v',
            'b', 'n', 'm', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
            'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X', 'C', 'V',
            'B', 'N', 'M'};

        public MurmurHash() {
        }

        public static long murmurHash(String key) {

            ByteBuffer buf = ByteBuffer.wrap(key.getBytes());
            int seed = 0x1234ABCD;

            ByteOrder byteOrder = buf.order();
            buf.order(ByteOrder.LITTLE_ENDIAN);

            long m = 0xc6a4a7935bd1e995L;
            int r = 47;

            long h = seed ^ (buf.remaining() * m);

            long k;
            while (buf.remaining() >= 8) {
                k = buf.getLong();

                k *= m;
                k ^= k >>> r;
                k *= m;

                h ^= k;
                h *= m;
            }

            if (buf.remaining() > 0) {
                ByteBuffer finish = ByteBuffer.allocate(8).order(
                    ByteOrder.LITTLE_ENDIAN);
                // for big-endian version, do this first:
                // finish.position(8-buf.remaining());
                finish.put(buf).rewind();
                h ^= finish.getLong();
                h *= m;
            }

            h ^= h >>> r;
            h *= m;
            h ^= h >>> r;

            buf.order(byteOrder);
            return Math.abs(h);
        }

        public static boolean isDigit(String strNum) {
            return strNum.matches("[0-9]+");
        }
    }

    public static class PartitionByUintMurmurHash extends DblePartitionAlgorithm {

        private static final long serialVersionUID = 3777423001153345948L;
        private static final String CNO_DEFAULT_LENGTH = "15";
        protected String partitionCount;
        protected String isDigitCheck;
        protected String shardingValueMaxLength;
        protected String leftPaddingChar;
        private long maxHashDivisor;
        public final static String simpleString = "0123456789";
        public final static String CHECK_CLOSE = "0";
        public final static String CHECK_OPEN = "1";
        public final static String DEFAULT_PADDING_CHAR = "NO";

        public PartitionByUintMurmurHash() {
        }

        public void setPartitionCount(String partitionCount) {
            this.partitionCount = partitionCount;
            //propertiesMap.put("partitionCount", partitionCount);
        }

        public String getIsDigitCheck() {
            if ("".equals(isDigitCheck) || isDigitCheck == null) {
                isDigitCheck = CHECK_OPEN;
            }
            return isDigitCheck;
        }

        public boolean isDigitCheck() {
            // default = 1(open)
            if (CHECK_CLOSE.equals(getIsDigitCheck())) {
                return false;
            } else {
                return true;
            }
        }

        public void setIsDigitCheck(String isDigitCheck) {
            this.isDigitCheck = isDigitCheck;
            //propertiesMap.put("isDigitCheck", CHECK_OPEN);
        }

        // padding char into column on left
        public void setLeftPaddingChar(String leftPaddingChar) {
            this.leftPaddingChar = leftPaddingChar;
            //propertiesMap.put("leftPaddingChar", this.leftPaddingChar);
        }

        public String getLeftPaddingChar() {
            if ("".equals(leftPaddingChar) || leftPaddingChar == null) {
                this.leftPaddingChar = DEFAULT_PADDING_CHAR;
            }
            return this.leftPaddingChar;
        }

        public String getShardingValueMaxLength() {
            if ("".equals(this.shardingValueMaxLength) || this.shardingValueMaxLength == null) {
                this.shardingValueMaxLength = CNO_DEFAULT_LENGTH;
            }
            return this.shardingValueMaxLength;
        }

        public void setShardingValueMaxLength(String shardingValueMaxLength) {
            this.shardingValueMaxLength = shardingValueMaxLength;
            //propertiesMap.put("shardingValueMaxLength", this.shardingValueMaxLength);
        }

        @Override
        public void init() {
            double partitionLog = Math.log(Double.valueOf(this.partitionCount)) / Math.log(2);
            this.maxHashDivisor = Math.round(Math.pow(2, 63 - partitionLog));
        }

        @Override
        public Integer calculate(String columnValue) {
            try {
                columnValue = checkColumn(columnValue);
                MurmurHash murmurHashObj = new MurmurHash();
                long hash = murmurHashObj.murmurHash(columnValue);
                return (int) mapping(hash, this.maxHashDivisor) - 1;
            } catch (Exception e) {
                throw new IllegalArgumentException("columnValue:'" + columnValue + "', Please check routeKey is legal.",
                    e);
            }
        }

        public String checkColumn(String columnValue) throws NumberFormatException {
            // customer number
            if ("".equals(columnValue) || columnValue == null) {
                // no customer number,just check null or "";
                try {
                    throw new IllegalArgumentException("routeField is null or \"\" !");
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            } else if (isDigitCheck()) {
                int columnLength = Integer.valueOf(getShardingValueMaxLength());
                // is digit?
                if (!MurmurHash.isDigit(columnValue.trim())) {
                    try {
                        throw new IllegalArgumentException("routeField is not digit !");
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                }
                // length check;default 1;stop padding which value is 0
                if (columnLength > 0) {
                    columnValue = addZeroForKey(columnValue.trim(), columnLength);
                }
            } else {
                // no left padding char
                if ("NO".equals(getLeftPaddingChar())) {
                    // origin keyValue
                    return columnValue;
                }
                // padding left char by "leftPaddingChar" with shardingValueMaxLength
                columnValue = addCharForKey(columnValue.trim(), Integer.valueOf(getShardingValueMaxLength()));
            }
            return columnValue;
        }

        @Override
        public Integer[] calculateRange(String beginValue, String endValue) {
            return new Integer[0];
        }

        @Override
        public void selfCheck() {
            if (this.partitionCount == null || this.partitionCount.length() < 1
                || Integer.valueOf(this.partitionCount) == 0) {
                throw new RuntimeException("set PartitionCount is null or is zero");
            }
            if ((Integer.valueOf(this.partitionCount) & (Integer.valueOf(this.partitionCount) - 1)) != 0) {
                throw new RuntimeException("PartitionCount is not Power of 2  or PartitionCount is zero.");
            }
        }

        public long mapping(long hashValue, long maxHashValue) {
            if (hashValue == 0L) {
                return 1L;
            }
            return Math.floorDiv(hashValue - 1, maxHashValue) + 1;
        }

        public String addZeroForKey(String str, int strLength) {
            int strLen = str.length();
            if (strLen > strLength) {
                throw new IllegalArgumentException("routeField is too long, over " + strLength);
            }
            StringBuffer sb = new StringBuffer();
            while (strLen < strLength) {
                sb.append("0");
                strLen++;
            }
            return sb.toString().concat(str);
        }

        public String addCharForKey(String str, int strLength) {
            int strLen = str.length();
            if (strLen > strLength) {
                try {
                    throw new IllegalArgumentException("routeField is too long, over " + strLength);
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
            }
            StringBuffer sb = new StringBuffer();
            while (strLen < strLength) {
                sb.append(getLeftPaddingChar());
                strLen++;
            }
            return sb.toString().concat(str);
        }
    }

    public UnitmurhashUdfImpl() {
        String partitionCount = "32";
        String isDigitCheck = null;
        String leftPaddingChar = null;
        String shardingValueMaxLength = null;
        init("32", isDigitCheck, leftPaddingChar, shardingValueMaxLength);
    }

    protected void init(String partitionCount,
                        String isDigitCheck,
                        String leftPaddingChar,
                        String shardingValueMaxLength
    ) {

        PartitionByUintMurmurHash unitMurmurHash = new PartitionByUintMurmurHash();
        if (partitionCount != null && !partitionCount.isEmpty()) {
            unitMurmurHash.setPartitionCount(partitionCount);
        }
        if (isDigitCheck != null && !isDigitCheck.isEmpty()) {
            unitMurmurHash.setIsDigitCheck(isDigitCheck);
        }
        if (leftPaddingChar != null && !leftPaddingChar.isEmpty()) {
            unitMurmurHash.setLeftPaddingChar(leftPaddingChar);
        }
        if (shardingValueMaxLength != null && !shardingValueMaxLength.isEmpty()) {
            unitMurmurHash.setShardingValueMaxLength(shardingValueMaxLength);
        }

        unitMurmurHash.init();
        this.routeFunc = unitMurmurHash;
    }

    @Override
    public Object compute(Object[] args) {
        return routeFunc.compute(args);
    }

}

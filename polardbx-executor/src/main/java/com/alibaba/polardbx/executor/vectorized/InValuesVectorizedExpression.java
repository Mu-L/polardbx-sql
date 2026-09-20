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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.executor.utils.fastutil.MemoryCountableIntOpenHashSet;
import com.alibaba.polardbx.executor.utils.fastutil.MemoryCountableLongOpenHashSet;
import com.alibaba.polardbx.executor.utils.fastutil.MemoryCountableObjectHashSet;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Set;

/**
 * Representation of in values
 */
public class InValuesVectorizedExpression extends AbstractVectorizedExpression {

    private static final Logger logger = LoggerFactory.getLogger(InValuesVectorizedExpression.class);

    private final int operandCount;
    private InValueSet inValueSet;
    private boolean hasNull;
    private boolean allNull;

    public InValuesVectorizedExpression(DataType colDataType, DataType<?> inDataType,
                                        List<RexNode> rexLiteralList, int outputIndex, boolean isLeftIntermediate) {
        super(inDataType, outputIndex, new VectorizedExpression[0]);
        this.operandCount = rexLiteralList.size() - 1;
        InValueSet inValueSet = null;
        this.hasNull = false;
        this.allNull = true;

        if (shouldBeConverted(inDataType, colDataType)) {
            buildSet(colDataType, inDataType, rexLiteralList, isLeftIntermediate);
        } else {
            // we get an unknown type-conversion.
            buildSetFallback(inDataType, rexLiteralList);
        }
    }

    private void buildSet(DataType colDataType, DataType<?> inDataType, List<RexNode> rexLiteralList,
                          boolean isLeftIntermediate) {
        InValueSet inValueSet;
        try {
            // try converting string into number for better performance
            inValueSet = new InValueSet(colDataType, operandCount);

            switch (colDataType.fieldType()) {
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATETIME2:
            case MYSQL_TYPE_TIMESTAMP: {
                if (!isLeftIntermediate) {
                    for (int operandIndex = 1; operandIndex <= operandCount; operandIndex++) {
                        Object value = ((RexLiteral) rexLiteralList.get(operandIndex)).getValue3();
                        OriginalTimestamp datetime =
                            (OriginalTimestamp) DateTimeType.DATE_TIME_TYPE_2.convertFrom(value);

                        if (datetime == null) {
                            this.hasNull = true;
                        } else {
                            long inConst = TimeStorage.writeTimestamp(datetime.getMysqlDateTime());
                            this.allNull = false;
                            inValueSet.add(inConst); // long-set.
                        }
                    }
                    break;
                }
                // to default.
            }
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE: {
                if (!isLeftIntermediate) {
                    for (int operandIndex = 1; operandIndex <= operandCount; operandIndex++) {
                        Object value = ((RexLiteral) rexLiteralList.get(operandIndex)).getValue3();
                        OriginalDate date = (OriginalDate) DataTypes.DateType.convertFrom(value);

                        if (date == null) {
                            this.hasNull = true;
                        } else {
                            long inConst = TimeStorage.writeDate(date.getMysqlDateTime());
                            this.allNull = false;
                            inValueSet.add(inConst); // long-set.
                        }
                    }
                    break;
                }
                // to default.
            }
            default: {
                for (int operandIndex = 1; operandIndex <= operandCount; operandIndex++) {
                    Object value = ((RexLiteral) rexLiteralList.get(operandIndex)).getValue3();
                    Object convertedValue = colDataType.convertFrom(value);
                    if (convertedValue == null) {
                        this.hasNull = true;
                    } else {
                        this.allNull = false;
                        inValueSet.add(convertedValue);
                    }
                }
            }
            }

            this.inValueSet = inValueSet;
        } catch (Throwable e) {
            logger.warn(e.getMessage());

            // if type-conversion throws an unknown error.
            // just fallback.
            buildSetFallback(inDataType, rexLiteralList);
        }
    }

    private void buildSetFallback(DataType<?> inDataType, List<RexNode> rexLiteralList) {
        InValueSet inValueSet;
        inValueSet = new InValueSet(inDataType, operandCount);
        for (int operandIndex = 1; operandIndex <= operandCount; operandIndex++) {
            Object value = ((RexLiteral) rexLiteralList.get(operandIndex)).getValue3();
            Object convertedValue = inDataType.convertFrom(value);
            if (convertedValue == null) {
                this.hasNull = true;
            } else {
                this.allNull = false;
                inValueSet.add(convertedValue);
            }
        }

        this.inValueSet = inValueSet;
    }

    private boolean shouldBeConverted(DataType<?> inDataType, DataType colDataType) {
        return DataTypeUtil.anyMatchSemantically(inDataType, DataTypes.CharType, DataTypes.VarcharType) &&
            (DataTypeUtil.anyMatchSemantically(colDataType, DataTypes.IntegerType, DataTypes.LongType)
                || DataTypeUtil.isMysqlTimeType(colDataType));
    }

    public static InValuesVectorizedExpression from(List<RexNode> rexLiteralList, int outputIndex) {
        return from(rexLiteralList, outputIndex, false);
    }

    /**
     * @param rexLiteralList start from index:1
     */
    public static InValuesVectorizedExpression from(List<RexNode> rexLiteralList, int outputIndex,
                                                    boolean isLeftInterMediate) {
        Preconditions.checkArgument(rexLiteralList.size() > 1,
            "Illegal in values, list size: " + rexLiteralList.size());
        DataType colDataType = new Field(rexLiteralList.get(0).getType()).getDataType();
        RexLiteral rexLiteral = (RexLiteral) rexLiteralList.get(1);
        Field field = new Field(rexLiteral.getType());
        return new InValuesVectorizedExpression(colDataType, field.getDataType(), rexLiteralList, outputIndex,
            isLeftInterMediate);
    }

    public int getOperandCount() {
        return operandCount;
    }

    public InValueSet getInValueSet() {
        return inValueSet;
    }

    public boolean hasNull() {
        return this.hasNull;
    }

    public boolean allNull() {
        return this.allNull;
    }

    @Override
    public void eval(EvaluationContext ctx) {

    }

    public static class InValueSet implements MemoryCountable {

        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(InValueSet.class).instanceSize();

        @FieldMemoryCounter(false)
        private final DataType dataType;
        @FieldMemoryCounter(false)
        private final SetType setType;
        /**
         * TODO: Slice hash set
         */
        private MemoryCountableObjectHashSet<Object> objSet = null;
        private MemoryCountableIntOpenHashSet intHashSet = null;
        private MemoryCountableLongOpenHashSet longHashSet = null;

        private boolean isSliceType;

        public InValueSet(DataType<?> dataType, int capacity) {
            this.dataType = dataType;
            this.setType = getSetType(dataType);
            switch (setType) {
            case INT:
                this.intHashSet = new MemoryCountableIntOpenHashSet(capacity);
                this.isSliceType = false;
                break;
            case LONG:
                this.longHashSet = new MemoryCountableLongOpenHashSet(capacity);
                this.isSliceType = false;
                break;
            case OTHERS:
                this.objSet = new MemoryCountableObjectHashSet<>(capacity);
                this.isSliceType = dataType instanceof SliceType;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        static SetType getSetType(DataType<?> dataType) {
            if (dataType == DataTypes.IntegerType) {
                return SetType.INT;
            }
            if (dataType == DataTypes.LongType || DataTypeUtil.isMysqlTimeType(dataType)) {
                return SetType.LONG;
            }
            return SetType.OTHERS;
        }

        /**
         * skip type check and null check
         * @param value not null
         */
        public void add(Object value) {
            switch (setType) {
            case INT:
                intHashSet.add((int) value);
                break;
            case LONG:
                longHashSet.add((long) value);
                break;
            case OTHERS:
                if (!(value instanceof Slice)) {
                    this.isSliceType = false;
                }
                objSet.add(value);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        public boolean isSliceType() {
            return isSliceType;
        }

        public boolean contains(int value) {
            switch (setType) {
            case INT:
                return intHashSet.contains(value);
            case LONG:
                return longHashSet.contains(value);
            case OTHERS:
                return objSet.contains(dataType.convertFrom(value));
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        public boolean contains(long value) {
            switch (setType) {
            case INT:
                if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                    return intHashSet.contains((int) value);
                } else {
                    return false;
                }
            case LONG:
                return longHashSet.contains(value);
            case OTHERS:
                return objSet.contains(dataType.convertFrom(value));
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        public boolean contains(Object value) {
            if (value == null) {
                return false;
            }
            switch (setType) {
            case INT:
                return intHashSet.contains(dataType.convertFrom(value));
            case LONG:
                return longHashSet.contains(dataType.convertFrom(value));
            case OTHERS:
                return objSet.contains(dataType.convertFrom(value));
            default:
                throw new UnsupportedOperationException("Unsupported in value set type: " + setType);
            }
        }

        public DataType getDataType() {
            return dataType;
        }

        @Override
        public long getMemoryUsage() {
            long size = INSTANCE_SIZE;
            switch (setType) {
            case INT:
                size += intHashSet.getMemoryUsage();
                break;
            case LONG:
                size += longHashSet.getMemoryUsage();
                break;
            case OTHERS:
                size += objSet.getMemoryUsage();
                break;
            }
            return size;
        }

        public ObjectOpenHashSet<Object> getObjSet() {
            return objSet;
        }

        enum SetType {
            INT,
            LONG,
            OTHERS
        }
    }
}

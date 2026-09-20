package com.alibaba.polardbx.optimizer.core.function.calc.dble;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedParameterizedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.DbleAlgorithmBuilder;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.DbleAlgorithmInitParams;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

/**
 * @author chenghui.lch
 *
 */
public class DbleRoute extends UserDefinedParameterizedJavaFunction {

    protected DbleAlgorithmInitParams algorithmParams = null;
    protected DblePartitionAlgorithm dbleAlgorithm = null;
    protected String algorithmName = null;

    public DbleRoute() {
    }

    public DbleRoute(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {TddlOperatorTable.DBLE_ROUTE.getName()};
    }

    @Override
    protected void initFunction(String initParams) {
        this.algorithmParams = DbleAlgorithmInitParams.buildFromJson(algorithmName, initParams);
        this.algorithmParams.getBuildParams().setActualPartitionCount(String.valueOf(this.partitionCount));
        this.dbleAlgorithm = DbleAlgorithmBuilder.buildAlgorithmByInitParams(this.algorithmParams);
        this.initParamsInfo = algorithmParams;
    }

    @Override
    public Object compute(Object[] args) {
        if (!isInited) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format("function %s has not init yet", getFunctionNames()[0]));
        }
        return dbleAlgorithm.compute(args);
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public void setAlgorithmName(String algorithmName) {
        this.algorithmName = algorithmName;
    }

    public DblePartitionAlgorithm getDbleAlgorithm() {
        return dbleAlgorithm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DbleRoute other = (DbleRoute) o;
        DblePartitionAlgorithm otherAlgorithm = other.getDbleAlgorithm();
        if (otherAlgorithm == null && this.dbleAlgorithm != null) {
            return false;
        } else if (otherAlgorithm != null && this.dbleAlgorithm == null) {
            return false;
        } else if (otherAlgorithm != null && this.dbleAlgorithm != null) {
            boolean isSameAlgorithm = otherAlgorithm.equals(this.dbleAlgorithm);
            return isSameAlgorithm;
        } else {
            if (!StringUtils.equals(other.getAlgorithmName(), this.algorithmName)) {
                return false;
            }
        }
        return true;
    }


}

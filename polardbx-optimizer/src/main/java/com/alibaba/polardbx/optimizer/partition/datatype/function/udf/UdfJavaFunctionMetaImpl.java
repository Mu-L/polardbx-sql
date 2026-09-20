package com.alibaba.polardbx.optimizer.partition.datatype.function.udf;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.function.SqlDbleRouteFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedParameterizedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DbleRoute;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.partition.datatype.function.FunctionInitParams;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.ParameterScope;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class UdfJavaFunctionMetaImpl implements UdfJavaFunctionMeta {

    protected UserDefinedJavaFunction udfJavaFunc;
    protected SqlOperator udfJavaFuncAst;
    protected List<DataType> allInputDataTypes = new ArrayList<>();
    protected List<RelDataType> allInputRelDataTypes = new ArrayList<>();
    protected boolean usingParameterizedUdf = false;
    protected FunctionInitParams initParams;

    public UdfJavaFunctionMetaImpl(String udfFuncName,
                                   SqlOperator udfFuncAst,
                                   FunctionInitParams initParams) {
        IScalarFunction udfJavaFuncObj = null;
        boolean useInitParams = initParams != null;
        if (udfFuncAst instanceof SqlDbleRouteFunction) {
            udfJavaFuncObj = new DbleRoute();
            ((DbleRoute) udfJavaFuncObj).setAlgorithmName(initParams.getFunctionName());
        } else {
            IScalarFunction udfFunc = null;
            if (useInitParams) {
                /**
                 * <pre>
                 *  Method of getNormalFunction return a new-Created a ScalarFunction object by calling constructor of javaUdf.
                 *  Here if a javaUdf use a init paraParams, then it must be treated as a
                 *  state function and need recreating ScalarFunction object for each build funcMeta
                 * </pre>
                 */
                udfFunc = JavaFunctionManager.getInstance().getNormalFunction(udfFuncName);
            } else {
                /**
                 * <pre>
                 *  Method of getJavaFunction return a ScalarFunction by its state-status in udf definition.
                 *  if udf use "NO STATE", then getJavaFunction return a ScalarFunction object from a common-cached ScalarFunction object;
                 *  if udf use "STATE", then getJavaFunction return a newCreated ScalarFunction object by calling its constructor of javaUdf
                 * </pre>
                 */
                udfFunc = JavaFunctionManager.getInstance().getJavaFunction(udfFuncName);
            }

            udfJavaFuncObj = udfFunc;
        }

        this.udfJavaFunc = (UserDefinedJavaFunction) udfJavaFuncObj;
        this.udfJavaFuncAst = udfFuncAst;
        initFuncMetaParams(udfFuncAst, initParams);
    }

    protected void initFuncMetaParams(SqlOperator udfJavaFuncAst, FunctionInitParams initParams) {
        usingParameterizedUdf = this.udfJavaFunc instanceof UserDefinedParameterizedJavaFunction;
        if (usingParameterizedUdf) {
            this.initParams = initParams;
            if (this.initParams != null) {
                String initParamsContent = this.initParams.getParamsContent();
                ((UserDefinedParameterizedJavaFunction) this.udfJavaFunc).setPartitionCount(
                    initParams.getPartitionCount());
                ((UserDefinedParameterizedJavaFunction) this.udfJavaFunc).init(initParamsContent);
            }
        }

        List<RelDataType> relDataTypes = ((SqlFunction) udfJavaFuncAst).getParamTypes();
        for (int i = 0; i < relDataTypes.size(); i++) {
            RelDataType relDt = relDataTypes.get(i);
            DataType dt = DataTypeUtil.calciteToDrdsType(relDt);
            allInputDataTypes.add(dt);
            allInputRelDataTypes.add(relDt);
        }
    }

    @Override
    public UserDefinedJavaFunction getUdfJavaFunction() {
        return this.udfJavaFunc;
    }

    @Override
    public SqlOperator getUdfJavaFunctionAst() {
        return this.udfJavaFuncAst;
    }

    @Override
    public List<DataType> getInputDataTypes() {
        List<DataType> dataTypes = new ArrayList<>();
        List<RelDataType> relDataTypes = ((SqlFunction) this.udfJavaFuncAst).getParamTypes();
        for (int i = 0; i < relDataTypes.size(); i++) {
            RelDataType relDt = relDataTypes.get(i);
            DataType dt = DataTypeUtil.calciteToDrdsType(relDt);
            dataTypes.add(dt);
        }
        return dataTypes;
    }

    @Override
    public DataType getOutputDataType() {
        return this.udfJavaFunc.getReturnType();
    }

    @Override
    public FunctionInitParams getUdfInitParams() {
        return initParams;
    }

    public void setInitParams(FunctionInitParams initParams) {
        this.initParams = initParams;
    }

}

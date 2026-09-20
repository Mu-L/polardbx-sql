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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.function.calc.cobar.builder.CobarAlgorithmType;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.DbleAlgorithmType;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.AbstractPartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.FunctionInitParams;
import com.alibaba.polardbx.optimizer.partition.datatype.function.FunctionType;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.datatype.function.udf.UdfJavaFunctionHelper;
import org.apache.calcite.sql.SqlColumnWithUdfParamsExpr;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUdfParamsExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class UdfHashPartRouter extends RangePartRouter {

    protected int partitionCount = 0;
    protected SearchDatumHasher hasher = null;

    /**
     * The route function of udf
     */

    protected List<ColumnMeta> partColMetaList;
    protected List<Boolean> checkPartColStringTypeFlags;
    protected List<SqlNode> partExprList;

    protected boolean foundUseUdfParams = false;
    protected SqlColumnWithUdfParamsExpr udfParamsExpr = null;
    protected PartitionIntFunction udfRouteFunction;

    public UdfHashPartRouter(Object[] sortedBoundObjArr, SearchDatumHasher hasher) {
        super(sortedBoundObjArr, new LongComparator());
        this.partitionCount = sortedBoundObjArr.length;
        this.hasher = hasher;
    }

    public void initRouter() {
        List<Integer> partColWithUdfParamsExprIndexList = new ArrayList<>();
        List<FunctionInitParams> functionInitParamsList = new ArrayList<>();
        extractFunctionInitParamsListFromPartitionExprListIfExists(this.partExprList, this.partitionCount,
            partColWithUdfParamsExprIndexList,
            functionInitParamsList);
        foundUseUdfParams = !functionInitParamsList.isEmpty();
        if (!foundUseUdfParams) {
            return;
        }
        /**
         * Only support one col use udf_params now
         */
        Integer partColWithUdfParamsExprIndex = partColWithUdfParamsExprIndexList.get(0);
        FunctionInitParams initParams = functionInitParamsList.get(0);

        this.udfParamsExpr = (SqlColumnWithUdfParamsExpr) partExprList.get(partColWithUdfParamsExprIndex);
        List<ColumnMeta> partColMetas = partColMetaList;
        this.checkPartColStringTypeFlags = new ArrayList<>();
        for (int i = 0; i < partColMetas.size(); i++) {
            ColumnMeta columnMeta = partColMetas.get(i);
            DataType dt = columnMeta.getDataType();
            Boolean isStringType = false;
            if (DataTypeUtil.isStringType(dt)) {
                isStringType = true;
            }
            this.checkPartColStringTypeFlags.add(isStringType);
        }
        FunctionType funcType = initParams.getFunctionType();
        if (funcType == FunctionType.DBLE_PARAMS) {
            SqlFunction sqlDbleRouteOp = TddlOperatorTable.DBLE_ROUTE;
            udfRouteFunction =
                PartitionFunctionBuilder.createPartFuncBySqlOperatorWithInitParams(sqlDbleRouteOp, partColMetas,
                    initParams);
        } else if (funcType == FunctionType.DBLE_EXT_PARAMS) {
            String udfFuncName = initParams.getFunctionName();
            SqlOperator udfOperator = UdfJavaFunctionHelper.getUdfOperatorByFunctionName(udfFuncName);
            if (udfOperator == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Failed to find the user-defined function `%s` for creating partitioned table with udf init params",
                        udfFuncName));
            }
            udfRouteFunction =
                PartitionFunctionBuilder.createPartFuncBySqlOperatorWithInitParams(udfOperator, partColMetas,
                    initParams);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("Failed to create partition table with udf init params, initParams is %s",
                    initParams));
        }
    }

    protected static FunctionInitParams fetchUdfInitParams(SqlColumnWithUdfParamsExpr columnWithUdfParamsExpr,
                                                           Integer partitionCount) {
        if (columnWithUdfParamsExpr == null) {
            return null;
        }

        SqlUdfParamsExpr udfParams = (SqlUdfParamsExpr) columnWithUdfParamsExpr.getUdfParams();
        SqlNode functionNameAst = udfParams.getFunctionName();
        SqlNode paramsContentAst = udfParams.getParamsContent();

        FunctionInitParams intiParams = new FunctionInitParams();
        String functionNameStr = SQLUtils.normalize(functionNameAst.toString());
        String paramsContentStr = SQLUtils.normalize(paramsContentAst.toString());

        String[] funcTypeAndFuncName = functionNameStr.split("/");
        FunctionType funcType = null;
        String funcName = null;
        if (funcTypeAndFuncName.length == 1) {
            funcType = FunctionType.NORMAL;
            funcName = funcTypeAndFuncName[0].trim();
        } else if (funcTypeAndFuncName.length == 2) {
            funcType = FunctionType.getFuncTypeByTypeName(funcTypeAndFuncName[0]);
            funcName = funcTypeAndFuncName[1].trim();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("Found invalid udf function name %s", functionNameStr));
        }
        String fixFuncName = funcName;
        FunctionType finalFunctionType = funcType;
        if (funcType == FunctionType.DBLE_PARAMS) {
            DbleAlgorithmType algorithmType = DbleAlgorithmType.getDbleAlgorithmTypeByName(funcName);
            fixFuncName = algorithmType.getAlgorithmAliasName();
        } else if (funcType == FunctionType.COBAR_PARAMS) {
            /**
             * Validate if funcName belong cobar route function
             */
            CobarAlgorithmType cobarAlgorithmType = CobarAlgorithmType.getCobarAlgorithmTypeByName(funcName);
            if (cobarAlgorithmType == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    String.format("cobar route function %s is not supported", funcName));
            }
            DbleAlgorithmType algorithmType = DbleAlgorithmType.getDbleAlgorithmTypeByName(funcName);
            fixFuncName = algorithmType.getAlgorithmAliasName();
            /**
             * Use dble_params to mock cobar_params
             */
            finalFunctionType = FunctionType.DBLE_PARAMS;
        } else if (funcType == FunctionType.COBAR_EXT_PARAMS) {
            /**
             * Use dble_ext_params to mock cobar_ext_params
             */
            finalFunctionType = FunctionType.DBLE_EXT_PARAMS;
        }

        intiParams.setParamsAst(udfParams);
        intiParams.setFunctionType(finalFunctionType);
        intiParams.setFunctionName(fixFuncName);
        intiParams.setParamsContent(paramsContentStr);
        intiParams.setPartitionCount(partitionCount);

        return intiParams;
    }

    @Override
    public RouterResult routePartitions(ExecutionContext ec, ComparisonKind comp, Object searchVal) {
        RouterResult rs = null;
        if (comp == ComparisonKind.EQUAL) {
            long hashVal = 0;
            if (foundUseUdfParams) {
                SearchDatumInfo queryVal = (SearchDatumInfo) searchVal;
                PartitionBoundVal oneColVal = queryVal.getDatumInfo()[0];
                PartitionField oneColFld = oneColVal.getValue();
                SessionProperties sessionProperties = SessionProperties.fromExecutionContext(ec);

                performUdfHashPreCheckIfNeed(ec, (AbstractPartitionField) oneColFld, sessionProperties);
                hashVal = udfRouteFunction.evalIntEndpoint(oneColFld, sessionProperties, null);
            } else {
                // Convert the searchVal from field space to hash space
                hashVal = hasher.calcHashCodeForUdfHashStrategy(ec, (SearchDatumInfo) searchVal);
            }
            rs = super.routePartitions(ec, comp, hashVal);
        } else {
            /**
             * Here just use ComparisonKind.NOT_EQUAL to generate full scan RouterResult
             */
            rs = super.routePartitions(ec, ComparisonKind.NOT_EQUAL, searchVal);
        }
        rs.strategy = PartitionStrategy.UDF_HASH;
        return rs;
    }

    private void performUdfHashPreCheckIfNeed(ExecutionContext ec, AbstractPartitionField oneColFld,
                                              SessionProperties sessionProperties) {
        boolean needPerformQueryValuePreCheck =
            ec.getParamManager().getBoolean(ConnectionParams.ROUTE_TUPLE_USE_PRECHECK_BY_UDF_FUNC);
        if (needPerformQueryValuePreCheck) {
            PartitionField strValFld = null;
            Object rawValueToBeforeStored = oneColFld.getRawValueToBeforeStore();
            strValFld = PartitionFieldBuilder.createField(DataTypes.VarcharType);
            if (rawValueToBeforeStored == null) {
                strValFld.setNull();
            } else {
                if (rawValueToBeforeStored instanceof io.airlift.slice.Slice) {
                    strValFld.store(rawValueToBeforeStored, DataTypes.VarcharType);
                } else {
                    String rawValueStrToBeforeStored = String.valueOf(rawValueToBeforeStored);
                    strValFld.store(rawValueStrToBeforeStored, DataTypes.StringType);
                }
            }
            // check rawValueToBeforeStored by udf route
            udfRouteFunction.evalIntEndpoint(strValFld, sessionProperties, null);
        }
    }

    public static SearchDatumInfo buildHashSearchDatumInfo(SearchDatumInfo queryValDatum,
                                                           SearchDatumHasher hasher,
                                                           ExecutionContext ec) {
        long hashVal = hasher.calcHashCodeForUdfHashStrategy(ec, queryValDatum);
        PartitionBoundVal[] boundValArr = new PartitionBoundVal[1];
        boundValArr[0] =
            PartitionInfoBuilder
                .buildOneHashBoundValByLong(ec, hashVal, hasher.getHashBndValDataType(),
                    PartFieldAccessType.QUERY_PRUNING);
        SearchDatumInfo hashValDatum = new SearchDatumInfo(boundValArr);
        return hashValDatum;
    }

    public PartitionIntFunction getUdfRouteFunction() {
        return udfRouteFunction;
    }

    public List<SqlNode> getPartExprList() {
        return partExprList;
    }

    public void setPartExprList(List<SqlNode> partExprList) {
        this.partExprList = partExprList;
    }

    public List<ColumnMeta> getPartColMetaList() {
        return partColMetaList;
    }

    public void setPartColMetaList(List<ColumnMeta> partColMetaList) {
        this.partColMetaList = partColMetaList;
    }

    public static void extractFunctionInitParamsListFromPartitionExprListIfExists(
        List<SqlNode> partExprListInput,
        Integer partitionCount,
        List<Integer> partColWithUdfParamsExprIndexListOutput,
        List<FunctionInitParams> funcInitParamsListOfEachPartExprOutput
    ) {
        if (partExprListInput != null) {
            for (int i = 0; i < partExprListInput.size(); i++) {
                SqlNode partExpr = partExprListInput.get(i);
                if (partExpr instanceof SqlColumnWithUdfParamsExpr) {
                    SqlColumnWithUdfParamsExpr udfParamsExpr = (SqlColumnWithUdfParamsExpr) partExpr;
                    if (partColWithUdfParamsExprIndexListOutput != null) {
                        partColWithUdfParamsExprIndexListOutput.add(i);
                    }
                    if (funcInitParamsListOfEachPartExprOutput != null) {
                        FunctionInitParams initParams = fetchUdfInitParams(udfParamsExpr, partitionCount);
                        funcInitParamsListOfEachPartExprOutput.add(initParams);
                    }
                }
            }
        }
    }

    public static boolean checkIfPartColUsingDbleInitParams(List<SqlNode> partExprList, Integer partitionCount) {
        List<FunctionInitParams> functionInitParamsList = new ArrayList<>();
        extractFunctionInitParamsListFromPartitionExprListIfExists(partExprList, partitionCount, null,
            functionInitParamsList);
        if (functionInitParamsList.isEmpty()) {
            return false;
        }
        FunctionInitParams initParams = functionInitParamsList.get(0);
        FunctionType functionType = initParams.getFunctionType();
        if (functionType != FunctionType.DBLE_PARAMS && functionType != FunctionType.DBLE_EXT_PARAMS) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UdfHashPartRouter other = (UdfHashPartRouter) o;

        if (other.foundUseUdfParams != this.foundUseUdfParams) {
            return false;
        }

        if (this.foundUseUdfParams) {
            if (other.udfRouteFunction != null && this.udfRouteFunction != null) {
                if (!udfRouteFunction.equals(other.udfRouteFunction)) {
                    return false;
                }
            } else if (other.udfRouteFunction == null && this.udfRouteFunction != null) {
                return false;
            } else if (other.udfRouteFunction != null && this.udfRouteFunction == null) {
                return false;
            }
        }

        return true;
    }
}

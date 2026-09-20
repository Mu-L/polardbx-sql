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

package com.alibaba.polardbx.optimizer.core.function.calc;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;


@SuppressWarnings("rawtypes")
public abstract class UserDefinedParameterizedJavaFunction extends UserDefinedJavaFunction {

    protected final Object lock = new Object();
    protected volatile boolean isInited = false;
    protected String initParams;
    protected Object initParamsInfo;
    // The partitionCount of (sub)partitionBy
    protected Integer partitionCount;

    protected UserDefinedParameterizedJavaFunction() {
        super();
        this.noState = false;
    }

    protected UserDefinedParameterizedJavaFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
        this.noState = false;
    }

    public void init(String initParams) {
        synchronized (lock) {
            if (isInited()) {
                return;
            }
            try {
                initFunction(initParams);
                isInited = true;
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    public boolean isInited() {
        return isInited;
    }

    public Object getInitParamsInfo() {
        return initParamsInfo;
    }

    protected abstract void initFunction(String initParams);

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UserDefinedParameterizedJavaFunction localUdf = this;
        UserDefinedParameterizedJavaFunction otherUdf = (UserDefinedParameterizedJavaFunction) o;

        String localUdfName = localUdf.getFunctionNames()[0];
        String otherUdfName = otherUdf.getFunctionNames()[0];
        if (!StringUtils.equals(localUdfName, otherUdfName)) {
            return false;
        }

        boolean useDbleRouteFunc = localUdfName.equalsIgnoreCase(TddlOperatorTable.DBLE_ROUTE.getName());
        if (!useDbleRouteFunc) {
            Object localInitParams = localUdf.getInitParamsInfo();
            Object otherInitParams = otherUdf.getInitParamsInfo();
            if(localInitParams != null && otherInitParams == null) {
                return false;
            } else if (localInitParams == null && otherInitParams != null) {
                return false;
            } else if(localInitParams != null && otherInitParams != null) {
                if (!localInitParams.equals(otherInitParams)) {
                    return false;
                }
            }
        }
        return true;
    }
    public void setPartitionCount(Integer partitionCount) {
        this.partitionCount = partitionCount;
    }
}

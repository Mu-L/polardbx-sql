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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;

import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartOpPredCounter extends RexShuttle {

    protected int opPredCnt = 0;

    protected Map<Integer, ParameterContext> params;

    public PartOpPredCounter(Map<Integer, ParameterContext> params) {
        this.params = params;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        //return super.visitLiteral(literal);  
        SqlKind kind = call.getKind();
        if (kind == SqlKind.OR || kind == SqlKind.AND || kind == SqlKind.IN) {
            for (int i = 0; i < call.getOperands().size(); i++) {
                call.getOperands().get(i).accept(this);
            }
        } else if (kind == SqlKind.ROW && params != null) {
            if (call.getOperands().size() == 1 && call.getOperands().get(0) instanceof RexDynamicParam) {
                RexDynamicParam dynamicParam = (RexDynamicParam) call.getOperands().get(0);
                int index = dynamicParam.getIndex();
                ParameterContext pc = params.get(index + 1);
                if (pc != null && pc.getValue() != null && pc.getValue() instanceof RawString) {
                    RawString rawString = (RawString) pc.getValue();
                    opPredCnt += rawString.size();
                }
            }
        } else {
            opPredCnt++;
        }
        return call;
    }

    public int getOpPredCnt() {
        return opPredCnt;
    }
}

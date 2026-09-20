/*
 * Copyright 2019 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * Maps CN-side vector validation failures to stable PolarDB-X function errors.
 */
public final class VectorErrorMapper {

    private VectorErrorMapper() {
    }

    public static TddlRuntimeException invalidVector(String message) {
        return new TddlRuntimeException(ErrorCode.ERR_FUNCTION, message);
    }

    public static TddlRuntimeException invalidArgument(String functionName, String message) {
        return invalidVector(functionName + ": " + message);
    }
}

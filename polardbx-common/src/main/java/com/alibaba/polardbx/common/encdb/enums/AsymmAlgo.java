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

package com.alibaba.polardbx.common.encdb.enums;

import java.util.Arrays;

public enum AsymmAlgo implements OrdinalEnum {
    RSA(1),
    SM2(2);

    private final int val;

    AsymmAlgo(int i) {
        this.val = i;
    }

    public static AsymmAlgo from(int i) {
        return Arrays.stream(AsymmAlgo.values())
            .filter(e -> e.val == i)
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("invalid value"));
    }

    @Override
    public int getVal() {
        return val;
    }
}

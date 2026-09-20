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

package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.common.encdb.enums.MsgType;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskAlgo;
import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskType;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;
import com.alibaba.polardbx.server.encdb.EncdbRuleFormat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.*;
import static com.alibaba.polardbx.gms.metadb.encdb.EncdbRule.*;

/**
 * @author pangzhaoxing
 */
public class EncdbImportRuleHandler implements EncdbHandler {

    @Override
    public JSONObject handle(JSONObject request, ServerConnection serverConnection) {
        EncdbMsgProcessor.checkUserPrivileges(serverConnection, true);

        List<EncdbRule> encdbRules = parseRequest(request);
        EncdbRuleManager.getInstance().insertEncRules(encdbRules);

        return EMPTY;
    }

    public static List<EncdbRule> parseRequest(JSONObject request) {
        JSONObject encRule =
            JSON.parseObject(new String(Utils.base64ToBytes(request.getString(ENC_RULE)), StandardCharsets.UTF_8));
        return EncdbRuleFormat.parseNewEncRule(encRule);
    }

}

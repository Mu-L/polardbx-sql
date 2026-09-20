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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.ISyncResultHandler;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;

import java.util.List;
import java.util.Map;

/**
 * sync的代理类,基于extenstion查找具体实现
 *
 * @author agapple 2015年3月26日 下午5:51:31
 * @since 5.1.19
 */
public class SyncManagerHelper {

    private static ISyncManager instance;

    static {
        instance = ExtensionLoader.load(ISyncManager.class);
    }

    // DDL TASK 内部的 sync 使用该方法
    public static List<List<Map<String, Object>>> syncThrowExceptions(IGmsSyncAction action, SyncScope scope) {
        DdlMetaLogUtil.DDL_META_LOG.info("sync. action:" + JSONObject.toJSONString(action));
        return sync(action, DefaultSchema.getSchemaName(), scope, true);
    }

    public static List<List<Map<String, Object>>> syncThrowExceptions(IGmsSyncAction action, String schema,
                                                                      SyncScope scope) {
        return sync(action, schema, scope, true);
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schema, SyncScope scope,
                                                       boolean throwExceptions) {
        return instance.sync(action, schema, scope, throwExceptions);
    }

    // 生命周期跟库生命周期不一致的 sync action 使用此方法，例如 baseline、统计信息等
    public static List<List<Map<String, Object>>> syncWithDefaultDb(IGmsSyncAction action, SyncScope scope) {
        return sync(action, SystemDbHelper.DEFAULT_DB_NAME, scope, true);
    }

    public static List<List<Map<String, Object>>> syncIgnoreExceptions(IGmsSyncAction action, SyncScope scope) {
        return syncIgnoreExceptions(action, DefaultSchema.getSchemaName(), scope);
    }

    public static List<List<Map<String, Object>>> syncIgnoreExceptions(IGmsSyncAction action, String schema,
                                                                       SyncScope scope) {
        return sync(action, schema, scope, false);
    }

    public static void syncIgnoreExceptions(IGmsSyncAction action, String schema, SyncScope scope,
                                            ISyncResultHandler handler) {
        instance.sync(action, schema, scope, handler, false);
    }

    public static List<Map<String, Object>> sync(IGmsSyncAction action, String schema, String serverKey) {
        return instance.sync(action, schema, serverKey);
    }

}

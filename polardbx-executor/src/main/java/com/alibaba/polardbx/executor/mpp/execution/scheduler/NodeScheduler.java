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

package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

public class NodeScheduler {

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;

    @Inject
    public NodeScheduler(InternalNodeManager nodeManager, NodeTaskMap nodeTaskMap) {
        this.nodeManager = nodeManager;
        this.nodeTaskMap = nodeTaskMap;
    }

    public NodeSelector createNodeSelector(Session session, int limit, RandomNodeMode randomNode,
                                           boolean columnarMode) {
        int maxSplitsPerNode =
            session.getClientContext().getParamManager().getInt(ConnectionParams.MPP_SCHEDULE_MAX_SPLITS_PER_NODE);

        boolean enableOSSRoundRobin =
            session.getClientContext().getParamManager()
                .getBoolean(ConnectionParams.ENABLE_OSS_FILE_CONCURRENT_SPLIT_ROUND_ROBIN);

        MppScope mppScope = ExecUtils.getMppSchedulerScope(!columnarMode);

        Set<InternalNode> nodes = new HashSet<>(nodeManager.getAllNodes().getAllWorkers(mppScope));

        boolean preferLocal =
            session.getClientContext().getParamManager().getBoolean(ConnectionParams.MPP_PREFER_LOCAL_NODE);

        if (columnarMode) {
            boolean enableTwoChoiceSchedule = session.getClientContext().getParamManager()
                .getBoolean(ConnectionParams.ENABLE_TWO_CHOICE_SCHEDULE);

            return new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, limit, maxSplitsPerNode,
                enableOSSRoundRobin,
                randomNode, enableTwoChoiceSchedule, preferLocal, session.getClientContext());
        } else {
            return new SimpleNodeSelector(nodeManager, nodeTaskMap, nodes, limit, maxSplitsPerNode, enableOSSRoundRobin,
                randomNode, preferLocal);
        }
    }

    public NodeSelector createNodeSelector(Session session, int limit, RandomNodeMode randomNode) {
        boolean columnarMode = session.getClientContext().getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCHEDULE);
        boolean enableTtlHybridSchedule = session.getClientContext().getParamManager()
            .getBoolean(ConnectionParams.ENABLE_TTL_HYBRID_SCHEDULE);
        if (columnarMode || session.getClientContext().getTtlQueryType() == null || !enableTtlHybridSchedule) {
            return createNodeSelector(session, limit, randomNode, columnarMode);
        } else {
            if (!session.getClientContext().getParamManager()
                .getBoolean(ConnectionParams.ALLOW_TTL_HYBRID_SCHEDULE_WITHOUT_COLUMNAR_NODE)
                && nodeManager.getAllNodes().getOtherActiveColumnarNodes().isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_MPP, "No columnar node available");
            }
            int rowNodeLimit = limit;
            if (ConfigDataMode.isMasterMode() && !session.getClientContext().getParamManager()
                .getBoolean(ConnectionParams.ENABLE_MASTER_MPP)) {
                rowNodeLimit = 1;
            }
            SimpleNodeSelector simpleNodeSelector =
                (SimpleNodeSelector) createNodeSelector(session, rowNodeLimit, randomNode, false);
            ColumnarNodeSelector columnarNodeSelector =
                (ColumnarNodeSelector) createNodeSelector(session, limit, randomNode, true);
            return new HybridNodeSelector(simpleNodeSelector, columnarNodeSelector);
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar.DropColumnarIndexInDatabaseJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndexInDatabase;

public class LogicalDropIndexInDatabaseHandler extends LogicalCommonDdlHandler {
    private static final Logger logger = LoggerFactory.getLogger(LogicalDropIndexInDatabaseHandler.class);

    public LogicalDropIndexInDatabaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalDropIndexInDatabase logicalDropIndexInDatabase = (LogicalDropIndexInDatabase) logicalDdlPlan;

        return buildDropColumnarIndexJob(logicalDropIndexInDatabase, executionContext);
    }

    private DdlJob buildDropColumnarIndexJob(LogicalDropIndexInDatabase logicalDropIndexInDatabase,
                                               ExecutionContext executionContext) {
        logicalDropIndexInDatabase.prepareData();

        return DropColumnarIndexInDatabaseJobFactory.create(logicalDropIndexInDatabase, executionContext);
    }
}

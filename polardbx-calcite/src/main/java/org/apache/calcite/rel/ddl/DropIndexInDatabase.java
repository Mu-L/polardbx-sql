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

package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDropIndexInDatabase;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class DropIndexInDatabase extends DDL {
    public DropIndexInDatabase(RelOptCluster cluster, RelTraitSet traitSet, SqlNode sqlNode, RelDataType rowType) {
        super(cluster, traitSet, null);
        this.sqlNode = sqlNode;
        this.rowType = rowType;
    }

    public static DropIndexInDatabase create(SqlDropIndexInDatabase dropDatabase, RelDataType rowType, RelOptCluster cluster) {
        return new DropIndexInDatabase(cluster, cluster.traitSetOf(Convention.NONE), dropDatabase, rowType);
    }

    @Override
    public DropIndexInDatabase copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new DropIndexInDatabase(this.getCluster(), traitSet, ((DropIndexInDatabase) inputs.get(0)).getAst(), rowType);
    }
}

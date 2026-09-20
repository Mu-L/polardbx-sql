package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * @author wumu
 */
public class AlterTableGhost extends DDL {

    protected AlterTableGhost(RelOptCluster cluster, RelTraitSet traits, SqlNode sqlNode, SqlNode tableName) {
        super(cluster, traits, null);
        this.sqlNode = sqlNode;
        this.setTableName(tableName);
    }

    public static AlterTableGhost create(RelOptCluster cluster, SqlNode sqlNode, SqlNode tableName) {
        return new AlterTableGhost(cluster, cluster.traitSet(), sqlNode, tableName);
    }

    @Override
    public AlterTableGhost copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableGhost(this.getCluster(), traitSet, this.sqlNode, this.getTableName());
    }
}

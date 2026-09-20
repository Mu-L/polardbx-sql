package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableToggleFullScan extends DDL {
    final List<SqlIdentifier> objectNames;
    final boolean enable;

    protected AlterTableToggleFullScan(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                       RelDataType rowType,
                                       List<SqlIdentifier> objectNames,
                                       SqlNode tableName,
                                       boolean enable) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
        this.objectNames = objectNames;
        this.setTableName(tableName);
        this.enable = enable;
    }

    public static AlterTableToggleFullScan create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                  RelDataType rowType, List<SqlIdentifier> objectNames,
                                                  SqlNode tableName, boolean enable) {

        return new AlterTableToggleFullScan(cluster, traits, ddl, rowType, objectNames, tableName, enable);
    }

    @Override
    public AlterTableToggleFullScan copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableToggleFullScan(this.getCluster(), traitSet, this.ddl, rowType, this.objectNames,
            getTableName(), enable);
    }

    public List<SqlIdentifier> getObjectNames() {
        return objectNames;
    }

    public boolean isEnable() {
        return enable;
    }
}

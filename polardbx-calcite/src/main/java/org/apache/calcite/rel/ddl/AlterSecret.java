package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlterSecret;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

public class AlterSecret extends DDL {

    private final String secretName;
    private final Map<String, String> properties;

    protected AlterSecret(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                          RelDataType rowType, String secretName,
                          Map<String, String> properties) {
        super(cluster, traits, ddl, rowType);
        this.secretName = secretName;
        this.properties = properties;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(secretName != null ? secretName : "secret", SqlParserPos.ZERO));
    }

    public static AlterSecret create(RelOptCluster cluster, SqlAlterSecret sqlNode,
                                     RelDataType rowType) {
        return new AlterSecret(cluster, cluster.traitSetOf(Convention.NONE), sqlNode, rowType,
            sqlNode.getSecretName(), sqlNode.getProperties());
    }

    @Override
    public AlterSecret copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new AlterSecret(getCluster(), traitSet, this.ddl, rowType, secretName, properties);
    }

    public String getSecretName() {
        return secretName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}

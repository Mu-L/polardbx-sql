package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDropSecret;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class DropSecret extends DDL {

    private final String secretName;
    private final boolean ifExists;

    protected DropSecret(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                         RelDataType rowType, String secretName, boolean ifExists) {
        super(cluster, traits, ddl, rowType);
        this.secretName = secretName;
        this.ifExists = ifExists;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(secretName != null ? secretName : "secret", SqlParserPos.ZERO));
    }

    public static DropSecret create(RelOptCluster cluster, SqlDropSecret sqlNode,
                                    RelDataType rowType) {
        return new DropSecret(cluster, cluster.traitSetOf(Convention.NONE), sqlNode, rowType,
            sqlNode.getSecretName(), sqlNode.isIfExists());
    }

    @Override
    public DropSecret copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DropSecret(getCluster(), traitSet, this.ddl, rowType, secretName, ifExists);
    }

    public String getSecretName() {
        return secretName;
    }

    public boolean isIfExists() {
        return ifExists;
    }
}

package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCreateSecret;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

public class CreateSecret extends DDL {

    private final String secretName;
    private final boolean ifNotExists;
    private final Map<String, String> properties;

    protected CreateSecret(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                           RelDataType rowType, String secretName, boolean ifNotExists,
                           Map<String, String> properties) {
        super(cluster, traits, ddl, rowType);
        this.secretName = secretName;
        this.ifNotExists = ifNotExists;
        this.properties = properties;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(secretName != null ? secretName : "secret", SqlParserPos.ZERO));
    }

    public static CreateSecret create(RelOptCluster cluster, SqlCreateSecret sqlNode,
                                      RelDataType rowType) {
        return new CreateSecret(cluster, cluster.traitSetOf(Convention.NONE), sqlNode, rowType,
            sqlNode.getSecretName(), sqlNode.isIfNotExists(),
            sqlNode.getProperties());
    }

    @Override
    public CreateSecret copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new CreateSecret(getCluster(), traitSet, this.ddl, rowType,
            secretName, ifNotExists, properties);
    }

    public String getSecretName() {
        return secretName;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}

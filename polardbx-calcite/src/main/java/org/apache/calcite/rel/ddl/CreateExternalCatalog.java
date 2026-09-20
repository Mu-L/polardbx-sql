package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCreateExternalCatalog;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

public class CreateExternalCatalog extends DDL {

    private final String catalogName;
    private final boolean ifNotExists;
    private final String connector;
    private final Map<String, String> properties;
    private final String secretName;
    private final String comment;

    protected CreateExternalCatalog(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                    RelDataType rowType, String catalogName, boolean ifNotExists,
                                    String connector, Map<String, String> properties,
                                    String secretName, String comment) {
        super(cluster, traits, ddl, rowType);
        this.catalogName = catalogName;
        this.ifNotExists = ifNotExists;
        this.connector = connector;
        this.properties = properties;
        this.secretName = secretName;
        this.comment = comment;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(catalogName != null ? catalogName : "external", SqlParserPos.ZERO));
    }

    public static CreateExternalCatalog create(RelOptCluster cluster, SqlCreateExternalCatalog sqlNode,
                                               RelDataType rowType) {
        return new CreateExternalCatalog(cluster, cluster.traitSetOf(Convention.NONE), sqlNode, rowType,
            sqlNode.getCatalogName(), sqlNode.isIfNotExists(),
            sqlNode.getConnector(), sqlNode.getProperties(),
            sqlNode.getSecretName(), sqlNode.getComment());
    }

    @Override
    public CreateExternalCatalog copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new CreateExternalCatalog(getCluster(), traitSet, this.ddl, rowType,
            catalogName, ifNotExists, connector, properties, secretName, comment);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getConnector() {
        return connector;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getSecretName() {
        return secretName;
    }

    public String getComment() {
        return comment;
    }
}

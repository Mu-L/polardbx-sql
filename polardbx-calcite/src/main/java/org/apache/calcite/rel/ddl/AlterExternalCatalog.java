package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlterExternalCatalog;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

public class AlterExternalCatalog extends DDL {

    private final String catalogName;
    private final Map<String, String> properties;
    private final String comment;

    protected AlterExternalCatalog(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                   RelDataType rowType, String catalogName,
                                   Map<String, String> properties, String comment) {
        super(cluster, traits, ddl, rowType);
        this.catalogName = catalogName;
        this.properties = properties;
        this.comment = comment;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(catalogName != null ? catalogName : "external", SqlParserPos.ZERO));
    }

    public static AlterExternalCatalog create(RelOptCluster cluster, SqlAlterExternalCatalog sqlNode,
                                              RelDataType rowType) {
        return new AlterExternalCatalog(cluster, cluster.traitSetOf(Convention.NONE), sqlNode, rowType,
            sqlNode.getCatalogName(), sqlNode.getProperties(), sqlNode.getComment());
    }

    @Override
    public AlterExternalCatalog copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new AlterExternalCatalog(getCluster(), traitSet, this.ddl, rowType,
            catalogName, properties, comment);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }
}

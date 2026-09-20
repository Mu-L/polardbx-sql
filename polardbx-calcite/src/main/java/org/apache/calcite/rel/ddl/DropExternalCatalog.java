package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDropExternalCatalog;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class DropExternalCatalog extends DDL {

    private final String catalogName;
    private final boolean ifExists;

    protected DropExternalCatalog(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                  RelDataType rowType, String catalogName, boolean ifExists) {
        super(cluster, traits, ddl, rowType);
        this.catalogName = catalogName;
        this.ifExists = ifExists;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(catalogName != null ? catalogName : "external", SqlParserPos.ZERO));
    }

    public static DropExternalCatalog create(RelOptCluster cluster, SqlDropExternalCatalog sqlNode,
                                             RelDataType rowType) {
        return new DropExternalCatalog(cluster, cluster.traitSetOf(Convention.NONE), sqlNode, rowType,
            sqlNode.getCatalogName(), sqlNode.isIfExists());
    }

    @Override
    public DropExternalCatalog copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DropExternalCatalog(getCluster(), traitSet, this.ddl, rowType, catalogName, ifExists);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isIfExists() {
        return ifExists;
    }
}

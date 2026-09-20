package com.alibaba.polardbx.optimizer.core.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * A {@link TableModify} specialization for INSERT into external tables.
 * <p>
 * Unlike {@link LogicalInsert} (which targets distributed DRDS tables and
 * requires shard writers), {@code LogicalExternalInsert} delegates the entire
 * write responsibility to a {@link TableSink} object. The sink carries all
 * connector-specific configuration; the executor dispatches to the appropriate
 * {@code ExternalTableInsertHandler} implementation based on the sink's runtime
 * type.
 * <p>
 * The node is produced by
 * {@link ToDrdsRelVisitor} whenever the target table has engine
 * {@code EXTERNAL} and the SQL statement is INSERT.
 */
public class LogicalExternalInsert extends TableModify {

    /**
     * Connector-specific write target. Carries the metadata needed by the
     * executor to perform the actual insert (target path, format, etc.).
     */
    private final TableSink tableSink;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a {@code LogicalExternalInsert} wrapping an existing
     * {@link TableModify} (INSERT/REPLACE) produced by
     * {@code TddlSqlToRelConverter.convertInsert()} and visiting the plan tree.
     *
     * @param modify the generic {@link TableModify} from Calcite
     * @param tableSink connector-specific write target; must not be {@code null}
     */
    public LogicalExternalInsert(TableModify modify, TableSink tableSink) {
        super(modify.getCluster(),
            modify.getTraitSet(),
            modify.getTable(),
            modify.getCatalogReader(),
            modify.getInput(),
            modify.getOperation(),
            null,  // updateColumnList (INSERT has none)
            null,  // sourceExpressionList
            modify.isFlattened(),
            modify.getKeywords(),
            modify.getBatchSize(),
            modify.getAppendedColumnIndex(),
            modify.getHints(),
            modify.getTableInfo());
        this.tableSink = tableSink;
    }

    /**
     * Full constructor used by {@link #copy}.
     */
    private LogicalExternalInsert(RelOptCluster cluster, RelTraitSet traitSet,
                                  RelOptTable table, Prepare.CatalogReader catalogReader,
                                  RelNode input, Operation operation,
                                  boolean flattened, List<String> keywords,
                                  TableInfo tableInfo, TableSink tableSink) {
        super(cluster, traitSet, table, catalogReader, input, operation,
            null, null, flattened, keywords, 0, null,
            new SqlNodeList(SqlParserPos.ZERO), tableInfo);
        this.tableSink = tableSink;
    }

    // -------------------------------------------------------------------------
    // Factory method
    // -------------------------------------------------------------------------

    /**
     * Creates a {@code LogicalExternalInsert} from an existing {@link TableModify}
     * together with an explicit {@link TableSink}.
     *
     * @param modify Calcite {@link TableModify} node (INSERT or REPLACE)
     * @param tableSink connector-specific sink; must not be {@code null}
     */
    public static LogicalExternalInsert create(TableModify modify, TableSink tableSink) {
        return new LogicalExternalInsert(modify, tableSink);
    }

    // -------------------------------------------------------------------------
    // Accessor
    // -------------------------------------------------------------------------

    public TableSink getTableSink() {
        return tableSink;
    }

    /**
     * Returns the logical table name (first entry in {@code targetTableNames}).
     */
    public String getLogicalTableName() {
        return getTargetTableNames().get(0);
    }

    /**
     * Returns the schema name derived from the table's qualified name.
     */
    public String getSchemaName() {
        List<String> qualifiedName = table.getQualifiedName();
        return qualifiedName.size() >= 2 ? qualifiedName.get(0) : null;
    }

    // -------------------------------------------------------------------------
    // RelNode overrides
    // -------------------------------------------------------------------------

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalExternalInsert(
            getCluster(), traitSet, table, catalogReader,
            sole(inputs), getOperation(),
            isFlattened(), getKeywords(),
            getTableInfo(), tableSink);
    }

    // -------------------------------------------------------------------------
    // Explain / serialization
    // -------------------------------------------------------------------------

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("tableSink", tableSink.display());
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "ExternalInsert");
        pw.item("table", getLogicalTableName());
        String sinkDisplay = tableSink.display();
        if (!sinkDisplay.isEmpty()) {
            pw.item("sink", sinkDisplay);
        }
        return pw;
    }
}

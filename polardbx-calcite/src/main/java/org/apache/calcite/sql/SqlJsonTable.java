package org.apache.calcite.sql;

import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

public class SqlJsonTable extends SqlCall {

    /**
     * The operator for JSON_TABLE function
     */
    public static final SqlJsonTableOperator OPERATOR = SqlJsonTableOperator.INSTANCE;

    // JSON table components using Calcite types
    private final SqlNode jsonExpr;
    private final SqlNode pathExpr;
    private final List<JsonTableColumn> columns;

    public static class JsonTableColumn extends SqlCall {
        private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("JSON_TABLE_COLUMN",
            SqlKind.JSON_TABLE);

        private final SqlIdentifier name;
        private final SqlDataTypeSpec dataType;
        private final SqlNode path;
        private final boolean ordinality;
        private final boolean exists;
        private final SqlNode onError;
        private final SqlNode onEmpty;
        @Getter
        @Setter
        private List<JsonTableColumn> nestedColumns;

        public JsonTableColumn(SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode path,
                               boolean ordinality, boolean exists, SqlNode onError, SqlNode onEmpty) {
            super(pos);
            this.name = name;
            this.dataType = dataType;
            this.path = path;
            this.ordinality = ordinality;
            this.exists = exists;
            this.onError = onError;
            this.onEmpty = onEmpty;
            this.nestedColumns = new ArrayList<>();
        }

        // Getters
        public SqlIdentifier getName() {
            return name;
        }

        public SqlDataTypeSpec getDataType() {
            return dataType;
        }

        public SqlNode getPath() {
            return path;
        }

        public boolean isOrdinality() {
            return ordinality;
        }

        public boolean isExists() {
            return exists;
        }

        public SqlNode getOnError() {
            return onError;
        }

        @Override
        public SqlOperator getOperator() {
            return OPERATOR;
        }

        @Override
        public List<SqlNode> getOperandList() {
            return ImmutableNullableList.of(
                name,
                dataType,
                path,
                ordinality ? SqlLiteral.createBoolean(true, getParserPosition()) : null,
                exists ? SqlLiteral.createBoolean(true, getParserPosition()) : null,
                onError,
                onEmpty,
                SqlUtil.wrapSqlNodeList(nestedColumns)
            );
        }

        @Override
        public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
            // Unparse column name
            if (name != null && name.getSimple().equals("NESTED")) {
                writer.keyword("NESTED");
            } else if (name != null) {
                name.unparse(writer, leftPrec, rightPrec);
            }

            // Unparse column data type
            if (dataType != null) {
                writer.keyword(" ");
                dataType.unparse(writer, leftPrec, rightPrec);
            }

            // Unparse column path
            if (path != null) {
                writer.keyword(" PATH ");
                path.unparse(writer, leftPrec, rightPrec);
            }

            // Handle special column types
            if (ordinality) {
                writer.keyword(" FOR ORDINALITY");
            }

            if (exists) {
                writer.keyword(" EXISTS");
            }

            // Handle ON ERROR and ON EMPTY clauses

            if (onError != null) {
                if (onError instanceof SqlLiteral && ((SqlLiteral) onError).getValue() == null) {
                    writer.keyword(" NULL ON ERROR");
                } else {
                    writer.keyword(" DEFAULT ");
                    onError.unparse(writer, leftPrec, rightPrec);
                    writer.keyword(" ON ERROR");
                }
            }

            if (onEmpty != null) {
                if (onEmpty instanceof SqlLiteral && ((SqlLiteral) onEmpty).getValue() == null) {
                    writer.keyword(" NULL ON EMPTY");
                } else {
                    writer.keyword(" DEFAULT ");
                    onEmpty.unparse(writer, leftPrec, rightPrec);
                    writer.keyword(" ON EMPTY");
                }
            }

            // Handle nested columns
            if (nestedColumns != null && !nestedColumns.isEmpty()) {
                writer.keyword(" COLUMNS (");

                boolean firstNested = true;
                for (JsonTableColumn nestedColumn : nestedColumns) {
                    if (!firstNested) {
                        writer.keyword(",");
                    }
                    firstNested = false;
                    nestedColumn.unparse(writer, leftPrec, rightPrec);
                }

                writer.keyword(")");
            }
        }
    }

    /**
     * Creates a SqlJsonTable node.
     *
     * @param jsonExpr JSON expression
     * @param pathExpr Path expression
     * @param columns Column definitions
     * @param pos Parser position
     */
    public SqlJsonTable(SqlNode jsonExpr, SqlNode pathExpr, List<JsonTableColumn> columns,
                        SqlParserPos pos) {
        super(pos);
        this.jsonExpr = jsonExpr;
        this.pathExpr = pathExpr;
        this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
    }

    // Getters
    public SqlNode getJsonExpr() {
        return jsonExpr;
    }

    public SqlNode getPathExpr() {
        return pathExpr;
    }

    public List<JsonTableColumn> getColumns() {
        return columns;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlJsonTable(jsonExpr, pathExpr, columns, pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("JSON_TABLE");
        writer.keyword("(");

        // Unparse the JSON expression
        if (jsonExpr != null) {
            jsonExpr.unparse(writer, leftPrec, rightPrec);
        }

        writer.keyword(",");

        // Unparse the path
        if (pathExpr != null) {
            pathExpr.unparse(writer, leftPrec, rightPrec);
        }

        // Unparse columns if any
        if (!columns.isEmpty()) {
            writer.keyword("COLUMNS");
            writer.keyword("(");
            boolean first = true;
            for (JsonTableColumn column : columns) {
                if (!first) {
                    writer.keyword(",");
                }
                first = false;
                column.unparse(writer, leftPrec, rightPrec);
            }
            writer.keyword(")");
        }

        writer.keyword(")");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // Validate the JSON expression and path
        if (jsonExpr != null) {
            jsonExpr.validate(validator, scope);
        }
        if (pathExpr != null) {
            pathExpr.validate(validator, scope);
        }
        // Validate column definitions
        for (JsonTableColumn column : columns) {
            if (column.getName() != null) {
                column.getName().validate(validator, scope);
            }
            if (column.getPath() != null) {
                column.getPath().validate(validator, scope);
            }
        }
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        if (!(node instanceof SqlJsonTable)) {
            return litmus.fail("{} != {}", this, node);
        }

        SqlJsonTable that = (SqlJsonTable) node;

        // Compare JSON expressions
        if (!SqlNode.equalDeep(this.jsonExpr, that.jsonExpr, litmus, context)) {
            return litmus.fail("{} != {}", this, node);
        }

        // Compare path expressions
        if (!SqlNode.equalDeep(this.pathExpr, that.pathExpr, litmus, context)) {
            return litmus.fail("{} != {}", this, node);
        }

        // Compare columns count
        if (this.columns.size() != that.columns.size()) {
            return litmus.fail("{} != {}", this, node);
        }

        // For now, do a simple comparison - in a full implementation,
        // we would compare each column's properties
        return litmus.succeed();
    }

    public SqlKind getKind() {
        return SqlKind.JSON_TABLE;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        // Return the main operands: JSON expression, path expression, and column definitions
        return ImmutableNullableList.of(
            jsonExpr,
            pathExpr,
            SqlUtil.wrapSqlNodeList(columns)
        );
    }

    /**
     * An operator describing a JSON_TABLE specification.
     */
    public static class SqlJsonTableOperator extends SqlOperator {
        private static final SqlJsonTableOperator INSTANCE = new SqlJsonTableOperator();

        private SqlJsonTableOperator() {
            super("JSON_TABLE", SqlKind.JSON_TABLE, 2, true, null, null, null);
        }

        public SqlSyntax getSyntax() {
            return SqlSyntax.FUNCTION;
        }

        public SqlCall createCall(
            SqlLiteral functionQualifier,
            SqlParserPos pos,
            SqlNode... operands) {
            assert functionQualifier == null;

            // JSON_TABLE expects at least 2 operands: JSON expression and path expression
            if (operands.length < 2) {
                throw new IllegalArgumentException("JSON_TABLE requires at least 2 operands");
            }

            // Convert SqlNode operands to SqlDynamicParam
            SqlNode jsonExpr = operands[0];
            SqlNode pathExpr = operands[1];

            // Handle column definitions if present
            List<JsonTableColumn> columns = new ArrayList<>();
            if (operands.length > 2 && operands[2] != null) {
                SqlNode columnsNode = operands[2];
                if (columnsNode instanceof SqlNodeList) {
                    SqlNodeList columnsList = (SqlNodeList) columnsNode;
                    for (SqlNode columnNode : columnsList) {
                        if (columnNode instanceof JsonTableColumn) {
                            columns.add((JsonTableColumn) columnNode);
                        }
                    }
                }
            }

            return new SqlJsonTable(jsonExpr, pathExpr, columns, pos);
        }

        public <R> void acceptCall(
            SqlVisitor<R> visitor,
            SqlCall call,
            boolean onlyExpressions,
            SqlBasicVisitor.ArgHandler<R> argHandler) {

            if (onlyExpressions) {
                // Visit only the expression operands, not structural elements
                for (Ord<SqlNode> operand : Ord.zip(call.getOperandList())) {
                    if (operand.e != null) {
                        argHandler.visitChild(visitor, call, operand.i, operand.e);
                    }
                }
            } else {
                // Visit all operands including structural elements
                super.acceptCall(visitor, call, onlyExpressions, argHandler);
            }
        }

        public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {

            final SqlJsonTable jsonTable = (SqlJsonTable) call;
            jsonTable.unparse(writer, leftPrec, rightPrec);
        }
    }
}
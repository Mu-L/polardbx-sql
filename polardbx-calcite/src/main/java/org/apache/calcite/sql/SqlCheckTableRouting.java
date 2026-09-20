package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @version 1.0
 */
public class SqlCheckTableRouting extends SqlDal { // Use DDL here to utilize async DDL framework.

    private static final SqlSpecialOperator OPERATOR = new SqlCheckTableRouting.SqlCheckTableRoutingOperator();

    private Boolean checkIndexRouting = false;
    private Boolean explain = false;
    private SqlNode indexName;
    private SqlNode tableName;
    private SqlNodeList partitions;

    public static class SqlCheckTableRoutingOperator extends SqlSpecialOperator {

        public SqlCheckTableRoutingOperator(){
            super("CHECK_TABLE_ROUTING", SqlKind.CHECK_TABLE_ROUTING);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Table", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Op", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_type", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_text", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }

    public SqlCheckTableRouting(SqlParserPos pos,
                                Boolean isCheckIndexRouting,
                                Boolean isExplain,
                                SqlNode tableName,
                                SqlNode indexName,
                                SqlNodeList partitions) {
        super(pos);
        this.checkIndexRouting = isCheckIndexRouting;
        this.explain = isExplain;
        this.tableName = tableName;
        this.indexName = indexName;
        this.partitions = partitions;

    }

    public SqlNode getIndexName() {
        return indexName;
    }

    public void setIndexName(SqlNode indexName) {
        this.indexName = indexName;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public void setTableName(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (checkIndexRouting) {
            writer.print("CHECK INDEX ROUTING");
            if (indexName != null) {
                indexName.unparse(writer, leftPrec, rightPrec);
            }

            if (partitions != null && !partitions.getList().isEmpty()) {
                writer.print(" PARTITION ");
                partitions.unparse(writer, leftPrec, rightPrec);

//                for (int i = 0; i < partitions.size(); i++) {
//                    if (i > 0) {
//                        writer.print(",");
//                    }
//                    SqlNode part = partitions.get(i);
//                    part.unparse(writer, leftPrec, rightPrec);
//                }
//                writer.print(")");
            }

            if (tableName != null) {
                writer.print("ON");
                tableName.unparse(writer, leftPrec, rightPrec);
            }
        } else {
            writer.print("CHECK TABLE ROUTING ");
            if (tableName != null) {
                tableName.unparse(writer, leftPrec, rightPrec);
            }

            if (partitions != null && !partitions.getList().isEmpty()) {
                writer.print("PARTITION ");
                partitions.unparse(writer, leftPrec, rightPrec);
//                final SqlWriter.Frame partListFrames = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
//                for (int i = 0; i < partitions.size(); i++) {
//                    if (i > 0) {
//                        writer.print(",");
//                    }
//                    SqlNode part = partitions.get(i);
//                    part.unparse(writer, leftPrec, rightPrec);
//                }
//                writer.endList(partListFrames);
            }
        }

        if (explain) {
            writer.print(" EXPLAIN = TRUE");
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList();
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHECK_TABLE_ROUTING;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public Boolean getCheckIndexRouting() {
        return checkIndexRouting;
    }

    public void setCheckIndexRouting(Boolean checkIndexRouting) {
        this.checkIndexRouting = checkIndexRouting;
    }


    public SqlNodeList getPartitions() {
        return partitions;
    }

    public void setPartitions(SqlNodeList partitions) {
        this.partitions = partitions;
    }

    public Boolean getExplain() {
        return explain;
    }

    public void setExplain(Boolean explain) {
        this.explain = explain;
    }

    public static class SqlCheckGlobalIndexOperator extends SqlSpecialOperator {

        public SqlCheckGlobalIndexOperator() {
            super("CHECK_GLOBAL_INDEX", SqlKind.CHECK_GLOBAL_INDEX);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Table", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Op", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_type", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_text", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCheckTableRouting(this.pos,
            checkIndexRouting, explain, tableName, indexName, partitions);
    }

}

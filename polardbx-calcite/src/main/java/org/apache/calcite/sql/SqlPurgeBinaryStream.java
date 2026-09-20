package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

/**
 * @version 1.0
 */
public class SqlPurgeBinaryStream extends SqlDal {
    private final SqlNode streamName;
    private final SqlOperator operator;

    public SqlPurgeBinaryStream(SqlParserPos pos, SqlNode streamName) {
        super(pos);
        this.streamName = streamName;
        this.operator = new SqlPurgeBinaryStreamOperator(SqlKind.PURGE_BINARY_STREAM);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword("PURGE BINARY STREAM ");
        writer.print(streamName.toString());
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.PURGE_BINARY_STREAM;
    }

    public SqlNode getStreamName() {
        return streamName;
    }

    public static class SqlPurgeBinaryStreamOperator extends SqlSpecialOperator {

        public SqlPurgeBinaryStreamOperator(SqlKind sqlKind) {
            super(sqlKind.name(), sqlKind);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));

            return typeFactory.createStructType(columns);
        }
    }
}

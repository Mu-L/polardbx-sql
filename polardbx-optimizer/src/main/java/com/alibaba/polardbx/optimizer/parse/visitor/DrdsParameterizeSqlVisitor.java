/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOver;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalUnit;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLUnaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.ExportParameterVisitorUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.LastInsertId;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime.Now;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.PreparedParamRef;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.util.NlsString;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class DrdsParameterizeSqlVisitor extends MySqlOutputVisitor {
    private static final BigInteger MAX_UNSIGNED_INT64 = new BigInteger(Long.toUnsignedString(0xffffffffffffffffL));

    private static final BigInteger MIN_SIGNED_INT64 = new BigInteger(Long.toString(-0x7fffffffffffffffL - 1));

    private boolean hasIn = false;

    private boolean enableDynamicValuesOptimization = false;

    public boolean isHasIn() {
        return hasIn;
    }

    public void setEnableDynamicValuesOptimization(boolean enableDynamicValuesOptimization) {
        this.enableDynamicValuesOptimization = enableDynamicValuesOptimization;
    }

    private static final BigInteger MAX_SIGNED_INT64 = BigInteger.valueOf(Long.MAX_VALUE);

    public static class UserDefVariable {

        private String name;

        public UserDefVariable(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static class SysDefVariable {

        private String name;

        private boolean global;

        public SysDefVariable(String name, boolean global) {
            this.name = name;
            this.global = global;
        }

        public String getName() {
            return name;
        }

        public boolean isGlobal() {
            return global;
        }

    }

    public static class ConstantVariable {

        private String name;

        private Object[] args;

        public ConstantVariable(String name, Object args[]) {
            this.name = name;
            this.args = args;
        }

        public String getName() {
            return name;
        }

        public Object[] getArgs() {
            return args;
        }

        @Override
        public String toString() {
            return "?";
        }
    }

    private static class FoldedValuesInfo {
        private final List<SQLCastExpr> templateCasts;
        private final List<List<Object>> columnValues;

        private FoldedValuesInfo(List<SQLCastExpr> templateCasts, List<List<Object>> columnValues) {
            this.templateCasts = templateCasts;
            this.columnValues = columnValues;
        }
    }

    private ExecutionContext executionContext;

    public DrdsParameterizeSqlVisitor(Appendable appender, boolean parameterized, ExecutionContext executionContext) {
        super(appender, parameterized);
        this.executionContext = executionContext;
        this.isMySQL80 = InstanceVersion.isMYSQL80();
        this.isMySQL80 = InstanceVersion.isMYSQL80();

        /*
         * We only consider session timezone variable, because currently it does not work for PolarDB-X
         * to set a global timezone variable.
         */
        String sessionTimezone;
        if (null != executionContext
            && MapUtils.isNotEmpty(executionContext.getServerVariables())
            && null != (sessionTimezone = (String) executionContext.getServerVariables().get("time_zone"))) {
            if ("SYSTEM".equalsIgnoreCase(sessionTimezone)) {
                sessionTimezone = (String) executionContext.getServerVariables().get("system_time_zone");
                if ("CST".equalsIgnoreCase(sessionTimezone)) {
                    sessionTimezone = "GMT+08:00";
                }
            }

            if (null != sessionTimezone) {
                final String trimmed = sessionTimezone.trim();
                if (trimmed.length() > 0 && ('+' == trimmed.charAt(0) || '-' == trimmed.charAt(0))) {
                    // Convert '+08:00' to 'GMT+08:00'
                    sessionTimezone = "GMT" + trimmed;
                } else if (!sessionTimezone.equals(trimmed)) {
                    sessionTimezone = trimmed;
                }
            }

            this.timezone = sessionTimezone;
        }
    }

    @Override
    public boolean visit(SQLInListExpr x) {
        hasIn = true;
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLValuesTableSource x) {
        if (!parameterized || !enableDynamicValuesOptimization) {
            return super.visit(x);
        }
        FoldedValuesInfo foldedValuesInfo = collectFoldedValuesInfo(x);
        if (foldedValuesInfo == null) {
            return super.visit(x);
        }
        printFoldedValuesTableSource(x, foldedValuesInfo);
        return false;
    }

    @Override
    public boolean visit(MySqlCharExpr x) {
        String mysqlCharset = x.getCharset();
        String mysqlCollate = x.getCollate();
        boolean isHex = x.isHex();

        if (this.parameterized && mysqlCharset != null) {
            print('?');
            incrementReplaceCunt();
            if (this.parameters != null) {
                String text;
                if (CharsetName.of(mysqlCharset) == CharsetName.BINARY) {
                    text = new String(x.getBinary(), StandardCharsets.ISO_8859_1); // store in latin1
                } else {
                    text = x.getText();
                }
                Charset sqlCharset = CharsetName.convertStrToJavaCharset(mysqlCharset);
                if (sqlCharset == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                        "unsupported mysql character set: " + mysqlCharset);
                } else {
                    SqlCollation sqlCollation =
                        new SqlCollation(sqlCharset, mysqlCollate, SqlCollation.Coercibility.EXPLICIT);
                    if (isHex) {
                        CharsetName charsetName = CharsetName.of(sqlCharset);
                        if (charsetName != CharsetName.BINARY) {
                            text = charsetName.toUTF16String(text);
                        }
                    }
                    NlsString value = new NlsString(text, mysqlCharset, sqlCollation);
                    this.parameters.add(value);
                }
            }
            return false;
        } else {
            return super.visit(x);
        }
    }

    @Override
    public boolean visit(SQLVariantRefExpr x) {
        String name = x.getName();

        if (StringUtils.startsWith(name, "@@") || x.isGlobal() || x.isSession()) {
            String varText;
            if (x.isGlobal() || x.isSession()) {
                varText = name;
            } else {
                varText = TStringUtil.substring(name, 2);
            }
            if (parameterized) {
                print('?');
                incrementReplaceCunt();

                String sysVariableName = varText;
                if ("identity".equalsIgnoreCase(varText)) {
                    sysVariableName = LastInsertId.NAME;
                }

                if (this.parameters != null) {
                    this.parameters.add(new SysDefVariable(sysVariableName, x.isGlobal()));
                }
            }
        } else if (StringUtils.startsWith(name, "@")) {
            if (parameterized) {
                String varText = TStringUtil.substring(name, 1);

                // TConnection will set an empty user def variables.
                if (executionContext != null && executionContext.getUserDefVariables() != null) {

                    String varName = varText.toLowerCase();

                    // https://dev.mysql.com/doc/refman/5.7/en/user-variables.html
                    // If you refer to a variable that has not been initialized, it has a value of NULL and a type of string.
                    if (!executionContext.getUserDefVariables().containsKey(varName)) {
                        executionContext.getUserDefVariables().put(varName, null);
                    }

                    print('?');
                    incrementReplaceCunt();

                    if (this.parameters != null) {
                        this.parameters.add(new UserDefVariable(varText));
                    } else {
                        super.visit(x);
                    }
                } else {
                    super.visit(x);
                }
            }
        } else if (StringUtils.equals(name, "?")) {
            visitPreparedParam(x);
        } else {
            super.visit(x);
        }
        return false;
    }

    protected void visitPreparedParam(SQLVariantRefExpr x) {
        if (parameters != null) {
            parameters.add(new PreparedParamRef(x.getIndex()));
        }
        print('?');
    }

    @Override
    public boolean visit(SQLSetStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        if (x.isConnectByRoot()) {
            this.print0(this.ucase ? "CONNECT_BY_ROOT " : "connect_by_root ");
        }

        SQLExpr expr = x.getExpr();
        if (expr instanceof SQLIdentifierExpr) {
            this.print0(((SQLIdentifierExpr) expr).getName());
        } else if (expr instanceof SQLPropertyExpr) {
            this.visit((SQLPropertyExpr) expr);
        } else {
            this.printExpr(expr, this.parameterized);
        }

        String alias = x.getAlias();
        if (alias != null && alias.length() > 0) {
            this.print0(this.ucase ? " AS " : " as ");
            char c0 = alias.charAt(0);
            if (alias.indexOf(' ') != -1 && c0 != '\"' && c0 != '\'' && c0 != '`') {
                this.print('\"');
                this.print0(alias);
                this.print('\"');
            } else {
                this.print0(alias);
            }
        } else if (!(expr instanceof SQLAllColumnExpr)) {
            if (expr instanceof SQLPropertyExpr && ((SQLPropertyExpr) expr).getName().equalsIgnoreCase("*")) {
                return false;
            }
            String aliasNew = null;
            this.print0(this.ucase ? " AS " : " as ");
            if (expr instanceof SQLPropertyExpr) {
                final String name = ((SQLPropertyExpr) expr).getName();
                if (name.length() > 2 && '`' == name.charAt(0) && '`' == name.charAt(name.length() - 1)) {
                    aliasNew = name;
                } else {
                    aliasNew = "`" + SQLUtils.normalizeNoTrim(name) + "`";
                }
            } else if (expr instanceof SQLCharExpr) {
                aliasNew = quoteString(getAliasNew(((SQLCharExpr) expr).getText()));
            } else if (expr instanceof SQLIdentifierExpr) {
                aliasNew = ((SQLIdentifierExpr) expr).getName();
            } else {
                SQLUtils.FormatOption option = new SQLUtils.FormatOption(SqlParameterizeUtils.parameterizeFeatures);
                option.setUppCase(isUppCase());
                aliasNew = quoteString(SQLUtils.toMySqlString(expr, option));
            }
            this.print0(aliasNew);
        }

        return false;
    }

    private static final Set<String> METHOD_SKIP_PARAMETERIZE = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        // Don't parameterize the fractional temporal function.
//        METHOD_SKIP_PARAMETERIZE.add("NOW");
        METHOD_SKIP_PARAMETERIZE.add("CURTIME");
        METHOD_SKIP_PARAMETERIZE.add("CURRENT_TIME");
        METHOD_SKIP_PARAMETERIZE.add("CURRENT_TIMESTAMP");
        METHOD_SKIP_PARAMETERIZE.add("LOCALTIME");
        METHOD_SKIP_PARAMETERIZE.add("LOCALTIMESTAMP");
        METHOD_SKIP_PARAMETERIZE.add("SYSDATE");

        METHOD_SKIP_PARAMETERIZE.add("UTC_DATE");
        METHOD_SKIP_PARAMETERIZE.add("UTC_TIME");
        METHOD_SKIP_PARAMETERIZE.add("UTC_TIMESTAMP");
    }

    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        final String function = x.getMethodName();
        final boolean originParameterized = this.parameterized;
        if (METHOD_SKIP_PARAMETERIZE.contains(function)) {
            this.parameterized = false;
        }

        if (parameterized && LastInsertId.NAME.equalsIgnoreCase(function) && x.getArguments().size() == 0) {
            print('?');
            incrementReplaceCunt();

            String sysVariableName = LastInsertId.NAME;

            if (this.parameters != null) {
                this.parameters.add(new SysDefVariable(sysVariableName, false));
            }
            return false;
        }

        if (parameterized && Now.NAME.equalsIgnoreCase(function) && this.parameters != null) {

            String sysVariableName = Now.NAME;

            try {
                if (x.getArguments() == null || x.getArguments().size() == 0) {
                    print("cast(? as datetime)");
                    this.parameters.add(new ConstantVariable(sysVariableName, new Integer[0]));
                } else {
                    print("cast(? as datetime(" + Integer.valueOf(x.getArguments().get(0).toString()) + " ) )");
                    this.parameters.add(new ConstantVariable(sysVariableName,
                        new Integer[] {Integer.valueOf(x.getArguments().get(0).toString())}));
                }
                incrementReplaceCunt();
            } catch (NumberFormatException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS);
            }
            return false;
        }

        try {
            return super.visit(x);
        } finally {
            this.parameterized = originParameterized;
        }
    }

    @Override
    public boolean visit(SQLUnaryExpr x) {
        final boolean originParameterized = this.parameterized;
        if (x.getOperator() == SQLUnaryOperator.Negative
            && (x.getExpr() instanceof SQLIntegerExpr || x.getExpr() instanceof SQLNumberExpr)) {
            // The format like -(int number)
            this.parameterized = false;
        }
        try {
            return super.visit(x);
        } finally {
            this.parameterized = originParameterized;
        }
    }

    // todo HIGH RISK
//    @Override
//    public boolean visit(SQLHexExpr x) {
//        final boolean originParameterized = this.parameterized;
//        this.parameterized = true;
//        try {
//            return super.visit(x);
//        } finally {
//            this.parameterized = originParameterized;
//        }
//    }

    @Override
    public boolean visit(SQLIntervalExpr x) {
        this.print0(this.ucase ? "INTERVAL " : "interval ");
        SQLExpr value = x.getValue();
        this.printExpr(value, false);
        SQLIntervalUnit unit = x.getUnit();
        if (unit != null) {
            this.print(' ');
            this.print0(this.ucase ? unit.name() : unit.name_lcase);
        }

        return false;
    }

    @Override
    protected void printInteger(SQLIntegerExpr x, boolean parameterized) {
        if (parameterized) {
            normalizeBigIntegerLiteral(x);
        }

        super.printInteger(x, parameterized);
    }

    // Keep folded and scalar parameter types identical for out-of-range integers.
    private void normalizeBigIntegerLiteral(SQLIntegerExpr x) {
        if (x.getNumber() instanceof BigInteger) {
            BigInteger number = (BigInteger) x.getNumber();

            // The boundary value of bigint is min value of longlong and max value of ulonglong.
            // otherwise, the big integer number will be recognized as decimal value.
            if (number.compareTo(MAX_UNSIGNED_INT64) > 0 || number.compareTo(MIN_SIGNED_INT64) < 0) {
                BigDecimal decimalNumber = new BigDecimal(number);
                x.setNumber(decimalNumber);
            } else if (number.compareTo(MAX_SIGNED_INT64) <= 0) {
                // for -9223372036854775808 ~ 9223372036854775807, use normal long value.
                x.setNumber(x.getNumber().longValue());
            }
        }
    }

    /**
     * Collect a foldable VALUES table source into per-column value arrays.
     * Returns null (fall back to the default row-by-row parameterization) unless every value is a
     * plain literal wrapped in CAST and all rows share the same CAST target type per column.
     */
    private FoldedValuesInfo collectFoldedValuesInfo(SQLValuesTableSource valuesTableSource) {
        List<SQLListExpr> rows = valuesTableSource.getValues();
        // Folding needs at least two rows; ORDER BY / LIMIT depend on row order and count,
        // which the single-tuple form cannot represent.
        if (rows == null || rows.size() <= 1 || valuesTableSource.getOrderBy() != null
            || valuesTableSource.getLimit() != null) {
            return null;
        }

        int columnCount = rows.get(0).getItems().size();
        if (columnCount == 0) {
            return null;
        }

        // The result is transposed: row-major AST values become column-major parameter arrays,
        // one array per output column. templateCasts[i] is the CAST of column i taken from row 0.
        List<SQLCastExpr> templateCasts = new ArrayList<>(columnCount);
        List<List<Object>> columnValues = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnValues.add(new ArrayList<>(rows.size()));
        }
        List<Object> exported = new ArrayList<>(1);

        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            SQLListExpr row = rows.get(rowIndex);
            if (row.getItems().size() != columnCount) {
                return null;
            }

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                SQLExpr item = row.getItems().get(columnIndex);
                // Only the typed form CAST(literal AS type) is foldable; it fixes the target
                // type of the single folded placeholder.
                if (!(item instanceof SQLCastExpr)) {
                    return null;
                }

                SQLCastExpr castExpr = (SQLCastExpr) item;
                // Rejects non-literals (?, expressions) and charset-tagged string literals.
                if (!isFoldableValuesAtom(castExpr.getExpr())) {
                    return null;
                }

                // Row 0 defines the per-column template; later rows must CAST to the same type,
                // otherwise one placeholder could not carry mixed types.
                if (rowIndex == 0) {
                    templateCasts.add(castExpr);
                } else if (!isSameCastTarget(templateCasts.get(columnIndex), castExpr)) {
                    return null;
                }

                // Apply the same out-of-range integer normalization as the scalar printInteger
                // path, so folded and unfolded parameters keep identical Java types.
                if (castExpr.getExpr() instanceof SQLIntegerExpr) {
                    normalizeBigIntegerLiteral((SQLIntegerExpr) castExpr.getExpr());
                }
                exported.clear();
                ExportParameterVisitorUtils.exportParameter(exported, castExpr.getExpr());
                // Exactly one literal must produce exactly one parameter value.
                if (exported.size() != 1) {
                    return null;
                }
                columnValues.get(columnIndex).add(exported.get(0));
            }
        }

        return new FoldedValuesInfo(templateCasts, columnValues);
    }

    private boolean isFoldableValuesAtom(SQLExpr expr) {
        if (expr instanceof MySqlCharExpr && ((MySqlCharExpr) expr).getCharset() != null) {
            // Charset-tagged literals are exported with collation metadata only by visit(MySqlCharExpr).
            return false;
        }
        return expr instanceof SQLLiteralExpr;
    }

    private boolean isSameCastTarget(SQLCastExpr left, SQLCastExpr right) {
        if (left.isTry() != right.isTry() || left.isHasArray() != right.isHasArray()) {
            return false;
        }
        return isSameDataType(left.getDataType(), right.getDataType());
    }

    /**
     * Exhaustive CAST-target comparison for folding, following the BasicTypeBuilders idiom:
     * only explicitly whitelisted types may fold, each family compares the attributes that are
     * semantically meaningful for it, and any unlisted type (JSON / ENUM / SET / BIT / spatial /
     * future additions) falls through to reject. Adding a foldable type requires opting in here.
     */
    private boolean isSameDataType(SQLDataType left, SQLDataType right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null || left.getClass() != right.getClass()
            || !(left instanceof SQLDataTypeImpl)) {
            return false;
        }
        SQLDataTypeImpl leftImpl = (SQLDataTypeImpl) left;
        SQLDataTypeImpl rightImpl = (SQLDataTypeImpl) right;

        String typeName = StringUtils.upperCase(leftImpl.getName());
        if (!typeName.equals(StringUtils.upperCase(rightImpl.getName()))) {
            return false;
        }

        switch (typeName) {
        case "TINYINT":
        case "SMALLINT":
        case "MEDIUMINT":
        case "INT":
        case "INTEGER":
        case "BIGINT":
        case "DECIMAL":
        case "FLOAT":
        case "DOUBLE":
            // Numeric types: precision/scale/display-width arguments plus unsigned/zerofill.
            return sameArguments(leftImpl, rightImpl)
                && leftImpl.isUnsigned() == rightImpl.isUnsigned()
                && leftImpl.isZerofill() == rightImpl.isZerofill();
        case "DATE":
        case "DATETIME":
        case "TIMESTAMP":
        case "TIME":
            // Temporal types: fractional-seconds argument plus timezone flags.
            return sameArguments(leftImpl, rightImpl)
                && Objects.equals(leftImpl.getWithTimeZone(), rightImpl.getWithTimeZone())
                && leftImpl.isWithLocalTimeZone() == rightImpl.isWithLocalTimeZone();
        case "CHAR":
        case "VARCHAR":
            // Character types: length argument plus charset/collation attributes.
            if (!sameArguments(leftImpl, rightImpl)
                || !(leftImpl instanceof SQLCharacterDataType)
                || !(rightImpl instanceof SQLCharacterDataType)) {
                return false;
            }
            SQLCharacterDataType leftChar = (SQLCharacterDataType) leftImpl;
            SQLCharacterDataType rightChar = (SQLCharacterDataType) rightImpl;
            return StringUtils.equalsIgnoreCase(leftChar.getCharSetName(), rightChar.getCharSetName())
                && StringUtils.equalsIgnoreCase(leftChar.getCollate(), rightChar.getCollate())
                && StringUtils.equalsIgnoreCase(leftChar.getCharType(), rightChar.getCharType())
                && leftChar.isHasBinary() == rightChar.isHasBinary()
                && Objects.equals(leftChar.getHints(), rightChar.getHints());
        case "BINARY":
        case "VARBINARY":
        case "TINYTEXT":
        case "TEXT":
        case "MEDIUMTEXT":
        case "LONGTEXT":
        case "TINYBLOB":
        case "BLOB":
        case "MEDIUMBLOB":
        case "LONGBLOB":
            // Binary and large-object types: only the length argument (empty for LOBs).
            return sameArguments(leftImpl, rightImpl);
        default:
            // Not a reviewed foldable type; fall back to per-row parameterization.
            return false;
        }
    }

    private boolean sameArguments(SQLDataTypeImpl left, SQLDataTypeImpl right) {
        List<SQLExpr> leftArgs = left.getArguments();
        List<SQLExpr> rightArgs = right.getArguments();
        if (leftArgs.size() != rightArgs.size()) {
            return false;
        }
        return leftArgs.equals(rightArgs);
    }

    private void printFoldedValuesTableSource(
        SQLValuesTableSource valuesTableSource, FoldedValuesInfo foldedValuesInfo) {
        if (valuesTableSource.isBracket()) {
            print('(');
        }
        print0(ucase ? "VALUES ROW(" : "values row(");
        for (int i = 0; i < foldedValuesInfo.templateCasts.size(); i++) {
            if (i != 0) {
                print0(", ");
            }
            printFoldedCast(foldedValuesInfo.templateCasts.get(i));
        }
        print(')');
        if (valuesTableSource.isBracket()) {
            print(')');
        }
        printValuesAlias(valuesTableSource);
        if (this.parameters != null) {
            this.parameters.addAll(foldedValuesInfo.columnValues);
        }
    }

    private void printFoldedCast(SQLCastExpr castExpr) {
        if (castExpr.isTry()) {
            print0(ucase ? "TRY_CAST(" : "try_cast(");
        } else {
            print0(ucase ? "CAST(" : "cast(");
        }
        print('?');
        incrementReplaceCunt();
        print0(ucase ? " AS " : " as ");
        castExpr.getDataType().accept(this);
        if (castExpr.isHasArray()) {
            print0(ucase ? " ARRAY" : " array");
        }
        print(')');
    }

    private void printValuesAlias(SQLValuesTableSource valuesTableSource) {
        String alias = valuesTableSource.getAlias();
        if (alias == null) {
            return;
        }
        print0(ucase ? " AS " : " as ");
        printName0(alias);
        List<SQLName> columns = valuesTableSource.getColumns();
        if (columns.size() > 0) {
            print0(" (");
            printAndAccept(columns, ", ");
            print(')');
        }
    }

    private String getAliasNew(String alias) {
        if (alias != null && alias.length() != 0) {
            char first = alias.charAt(0);
            if (first != '"' && first != '\'') {
                return alias;
            } else if (alias.length() == 1) {
                if (alias.charAt(0) == '\'') {
                    return "'\\" + alias.charAt(0) + "'";
                } else {
                    return "\'" + alias.charAt(0) + "\'";
                }
            } else {
                char[] chars = new char[(alias.length() - 2) * 2 + 4];
                int len = 1;

                for (int i = 1; i < alias.length() - 1; ++i) {
                    char ch = alias.charAt(i);
                    if (ch == '\\') {
                        ++i;
                        ch = alias.charAt(i);
                    }
                    chars[len++] = ch;
                }
                chars[0] = alias.charAt(0);
                chars[len++] = alias.charAt(alias.length() - 1);
                return new String(chars, 0, len);
            }
        } else {
            return alias;
        }
    }

    private String quoteString(String x) {
        boolean usingAnsiMode = false;
        int stringLength = x.length();
        StringBuffer buf = new StringBuffer((int) (x.length() * 1.1));
        buf.append('\'');

        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);
            switch (c) {
            case 0:
                buf.append('\\');
                buf.append('0');
                break;
            case '\n':
                buf.append('\\');
                buf.append('n');
                break;
            case '\r':
                buf.append('\\');
                buf.append('r');
                break;
            case '\t':
                buf.append('\\');
                buf.append('t');
                break;
            case '\\':
                buf.append('\\');
                buf.append('\\');
                break;
            case '\'':
                buf.append('\\');
                buf.append('\'');
                break;
            case '"':
                if (usingAnsiMode) {
                    buf.append('\\');
                }
                buf.append('"');
                break;
            case '\032':
                buf.append('\\');
                buf.append('Z');
                break;
            default:
                buf.append(c);
            }
        }

        buf.append('\'');
        return buf.toString();
    }

    @Override
    public boolean visit(SQLOver x) {
        boolean parameterized = this.parameterized;
        this.parameterized = false;
        boolean visit = super.visit(x);
        this.parameterized = parameterized;
        return visit;
    }

    @Override
    public void postVisit(SQLObject x) {
        // do nothing
    }
}

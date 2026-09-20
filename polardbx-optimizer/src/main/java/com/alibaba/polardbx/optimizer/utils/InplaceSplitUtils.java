package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class InplaceSplitUtils {
    final static Set<String> SUPPORT_NUMBER_SQL_TYPES = new HashSet<>();
    final static Set<String> SUPPORT_DATE_TIME_DATA_TYPES = new HashSet<>();
    final static Set<String> SUPPORT_BINARY_SQL_TYPES = new HashSet<>();
    final static Map<String, Set<String>> SUPPORT_CHARSET_COLLATIONS_FOR_String_TYPE =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    final static Set<String> SUPPORT_CHARSETS = new HashSet<>();

    static {
        SUPPORT_NUMBER_SQL_TYPES.add(SqlTypeName.TINYINT.name());
        SUPPORT_NUMBER_SQL_TYPES.add(SqlTypeName.TINYINT_UNSIGNED.name());
        SUPPORT_NUMBER_SQL_TYPES.add(SqlTypeName.SMALLINT.name());
        SUPPORT_NUMBER_SQL_TYPES.add(SqlTypeName.SMALLINT_UNSIGNED.name());
        SUPPORT_NUMBER_SQL_TYPES.add(SqlTypeName.INTEGER.name());
        SUPPORT_NUMBER_SQL_TYPES.add(SqlTypeName.INTEGER_UNSIGNED.name());
        SUPPORT_NUMBER_SQL_TYPES.add(SqlTypeName.BIGINT.name());
        SUPPORT_NUMBER_SQL_TYPES.add(SqlTypeName.BIGINT_UNSIGNED.name());

        SUPPORT_DATE_TIME_DATA_TYPES.add(SqlTypeName.DATE.name());
        SUPPORT_DATE_TIME_DATA_TYPES.add(SqlTypeName.DATETIME.name());
        SUPPORT_DATE_TIME_DATA_TYPES.add(SqlTypeName.TIMESTAMP.name());

        SUPPORT_BINARY_SQL_TYPES.add(SqlTypeName.VARBINARY.name());
        SUPPORT_BINARY_SQL_TYPES.add(SqlTypeName.BINARY.name());
        // Initialize supported charsets
        /*SUPPORT_CHARSETS.add(CharsetName.UTF8.name());
        SUPPORT_CHARSETS.add(CharsetName.UTF8MB4.name());
        SUPPORT_CHARSETS.add(CharsetName.GBK.name());
        SUPPORT_CHARSETS.add(CharsetName.GB18030.name());
        SUPPORT_CHARSETS.add(CharsetName.LATIN1.name());
        SUPPORT_CHARSETS.add(CharsetName.ASCII.name());
        SUPPORT_CHARSETS.add(CharsetName.BINARY.name());*/

        // Initialize charset and collations mapping
        Set<String> asciiCollations = new HashSet<>();
        asciiCollations.add(CollationName.ASCII_BIN.name());
        SUPPORT_CHARSET_COLLATIONS_FOR_String_TYPE.put(CharsetName.ASCII.name(), asciiCollations);

        Set<String> latin1Collations = new HashSet<>();
        latin1Collations.add(CollationName.LATIN1_BIN.name());
        latin1Collations.add(CollationName.LATIN1_DANISH_CI.name());
        latin1Collations.add(CollationName.LATIN1_GENERAL_CI.name());
        latin1Collations.add(CollationName.LATIN1_GENERAL_CS.name());
        latin1Collations.add(CollationName.LATIN1_SPANISH_CI.name());
        SUPPORT_CHARSET_COLLATIONS_FOR_String_TYPE.put(CharsetName.LATIN1.name(), latin1Collations);

        Set<String> utf8mb4Collations = new HashSet<>();
        utf8mb4Collations.add(CollationName.UTF8MB4_0900_AI_CI.name());
        utf8mb4Collations.add(CollationName.UTF8MB4_BIN.name());
        utf8mb4Collations.add(CollationName.UTF8MB4_GENERAL_CI.name());
        utf8mb4Collations.add(CollationName.UTF8MB4_UNICODE_520_CI.name());
        utf8mb4Collations.add(CollationName.UTF8MB4_UNICODE_CI.name());
        utf8mb4Collations.add(CollationName.UTF8MB4_ZH_0900_AS_CS.name());
        SUPPORT_CHARSET_COLLATIONS_FOR_String_TYPE.put(CharsetName.UTF8MB4.name(), utf8mb4Collations);
    }

    public static boolean checkCharSetAndCollationSupport(TableMeta tableMeta,
                                                          boolean isEnableInplaceBackfill,
                                                          boolean isFirstLevelActiveKey,
                                                          ExecutionContext ec) {
        if (isEnableInplaceBackfill && tableMeta.getPartitionInfo() != null
            && tableMeta.getPartitionInfo().getPartitionBy() != null
            && tableMeta.getPartitionInfo().getPartitionBy().getAllLevelActualPartCols() != null) {
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            boolean firstLevelActiveKey = isFirstLevelActiveKey;
            int level = firstLevelActiveKey ? 0 : 1;
            List<String> columnsName = partitionInfo.getPartitionBy().getAllLevelActualPartCols().get(level);
            int supportType = ec.getParamManager().getInt(ConnectionParams.SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL);
            // 根据注释说明，支持的数据类型位表示：
            // 1: number, 10: string, 100: date, 1000: binary
            // 示例: 11: number+string 101: number+date 110: string+date 1001: number+binary  1111: string+date+binary+number
            boolean supportNumberType = (supportType % 10) == 1;
            boolean supportStringType = ((supportType / 10) % 10) == 1;
            boolean supportDateType = ((supportType / 100) % 10) == 1;
            boolean supportBinaryType = ((supportType / 1000) % 10) == 1;
            if (columnsName != null) {
                for (String columnName : columnsName) {
                    ColumnMeta columnMeta = tableMeta.getColumn(columnName);
                    if (columnMeta != null && columnMeta.getDataType() != null) {
                        DataType type = columnMeta.getDataType();
                        SqlTypeName sqlTypeName = columnMeta.getField().getRelType() != null ?
                            columnMeta.getField().getRelType().getSqlTypeName() : null;
                        if (DataTypeUtil.isStringType(type)) {
                            if (!supportStringType) {
                                return false;
                            }
                            String charsetName = columnMeta.getDataType().getCharsetName() != null ?
                                columnMeta.getDataType().getCharsetName().name() : null;
                            if (charsetName != null) {
                                if (SUPPORT_CHARSET_COLLATIONS_FOR_String_TYPE.containsKey(charsetName)) {
                                    String collationName = columnMeta.getDataType().getCollationName() != null ?
                                        columnMeta.getDataType().getCollationName().name() : null;
                                    if (collationName != null &&
                                        SUPPORT_CHARSET_COLLATIONS_FOR_String_TYPE.get(charsetName)
                                            .contains(collationName)) {
                                        continue;
                                    }
                                }
                                return false;
                            } else {
                                return false;
                            }
                        } else if (DataTypeUtil.isDateType(type)) {
                            // 检查日期时间类型是否受支持
                            if (!supportDateType) {
                                return false;
                            }
                            if (sqlTypeName != null && SUPPORT_DATE_TIME_DATA_TYPES.contains(sqlTypeName.name())) {
                                continue;
                            } else {
                                return false;
                            }
                        } else if (DataTypeUtil.isNumberSqlType(type)) {
                            // 检查数值类型是否受支持
                            if (!supportNumberType) {
                                return false;
                            }
                            if (sqlTypeName != null && SUPPORT_NUMBER_SQL_TYPES.contains(sqlTypeName.name())) {
                                continue;
                            } else {
                                return false;
                            }
                        } else if (DataTypeUtil.isBinaryType(type)) {
                            // 检查二进制类型是否受支持
                            if (!supportBinaryType) {
                                return false;
                            }
                            if (sqlTypeName != null && SUPPORT_BINARY_SQL_TYPES.contains(sqlTypeName.name())) {
                                continue;
                            } else {
                                return false;
                            }
                        } else {
                            // 其他不支持的类型
                            return false;
                        }
                    }
                }
            }
        }
        return isEnableInplaceBackfill;
    }

    public static boolean supportInplaceBackfill(String schemaName, String tableName, PartitionByDefinition partBy,
                                                 boolean useChangeSet, ExecutionContext ec) {
        PartitionStrategy strategy = partBy.getStrategy();
        PartitionIntFunction partFunc = partBy.getPartIntFunc();
        boolean isSplitHashPartition = strategy.isHashed();
        boolean enableInplaceBackfill = ec.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_INPLACE_BACKFILL);
        TableMeta tm = ec.getSchemaManager(schemaName).getTable(tableName);
        boolean hasPK = tm.isHasPrimaryKey();
        boolean isMySQL80 = InstanceVersion.isMYSQL80();
        boolean hasPartFunc = partFunc != null;
        boolean useNotTemplatePart =
            partBy.getSubPartitionBy() != null && !partBy.getSubPartitionBy().isUseSubPartTemplate();
        return !useNotTemplatePart && !hasPartFunc
            && isSplitHashPartition & enableInplaceBackfill & useChangeSet & hasPK && isMySQL80;
    }
}
package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnarVersionChainPruner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarVersionChainPruner.class);
    private static final String ITEM_SEPARATOR = ",";
    private static final String KEY_VALUE_SEPARATOR = ":";
    private static final String VALID_STRING_FORMAT =
        "prune string format error, expected format: schemaName.tableId:time_in_days";

    /**
     * <schemaName, tableId> -> day
     * -- GETTER --
     * Gets the parsed pruner map.
     */
    @Getter
    private volatile Map<Pair<String, Long>, Integer> prunerMap;

    private String prunerString;

    public ColumnarVersionChainPruner() {
        this.prunerString = InstConfUtil.getOriginVal(ConnectionParams.COLUMNAR_VERSION_CHAIN_PRUNER);
        this.prunerMap = parsePrunerString(prunerString);
    }

    public static void checkPruneStringValid(String prunerString) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            if (prunerString == null || prunerString.trim().isEmpty()) {
                return;
            }

            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);

            String[] items = prunerString.split(ITEM_SEPARATOR);
            for (String item : items) {
                item = item.trim();
                if (item.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, VALID_STRING_FORMAT);
                }
                // Find the last colon to separate time from the schema.table part
                int lastColonIndex = item.lastIndexOf(KEY_VALUE_SEPARATOR);
                if (lastColonIndex <= 0 || lastColonIndex == item.length() - 1) {
                    // Invalid format
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, VALID_STRING_FORMAT);
                }
                String schemaTablePart = item.substring(0, lastColonIndex);
                String timeStr = item.substring(lastColonIndex + 1);
                // Parse number of days pruned
                try {
                    Integer.parseInt(timeStr);
                } catch (NumberFormatException e) {
                    // Invalid time format
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, VALID_STRING_FORMAT);
                }

                // Find the last dot to separate schema and table names
                int firstDotIndex = schemaTablePart.indexOf('.');
                if (firstDotIndex <= 0 || firstDotIndex == schemaTablePart.length() - 1) {
                    // Invalid format
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, VALID_STRING_FORMAT);
                }

                String schemaName = schemaTablePart.substring(0, firstDotIndex);
                long tableId = Long.parseLong(schemaTablePart.substring(firstDotIndex + 1));

                List<ColumnarTableMappingRecord> columnarTableMappingRecords =
                    columnarTableMappingAccessor.querySchemaTableId(schemaName, tableId);

                if (columnarTableMappingRecords == null || columnarTableMappingRecords.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                        String.format("Table %s.%d does not exist", schemaName, tableId));
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parses the prunerString in the format:
     * schemaName1.tableId1:time1,schemaName2.tableId2:time2,...
     *
     * @param prunerString the string to parse
     */
    private Map<Pair<String, Long>, Integer> parsePrunerString(String prunerString) {
        Map<Pair<String, Long>, Integer> result = new HashMap<>();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            if (prunerString == null || prunerString.trim().isEmpty()) {
                return result;
            }

            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);

            String[] items = prunerString.split(ITEM_SEPARATOR);
            for (String item : items) {
                item = item.trim();
                if (item.isEmpty()) {
                    continue;
                }

                // Find the last colon to separate time from the schema.table part
                int lastColonIndex = item.lastIndexOf(KEY_VALUE_SEPARATOR);
                if (lastColonIndex <= 0 || lastColonIndex == item.length() - 1) {
                    // Invalid format, skip this item
                    continue;
                }

                String schemaTablePart = item.substring(0, lastColonIndex);
                String timeStr = item.substring(lastColonIndex + 1);

                // Parse number of days pruned
                int days;
                try {
                    days = Integer.parseInt(timeStr);
                } catch (NumberFormatException e) {
                    // Invalid time format, skip this item
                    continue;
                }

                // Find the last dot to separate schema and table names
                int firstDotIndex = schemaTablePart.indexOf('.');
                if (firstDotIndex <= 0 || firstDotIndex == schemaTablePart.length() - 1) {
                    // Invalid format, skip this item
                    continue;
                }

                String schemaName = schemaTablePart.substring(0, firstDotIndex);
                long tableId = Long.parseLong(schemaTablePart.substring(firstDotIndex + 1));

                result.put(new Pair<>(schemaName, tableId), days);
            }
        } catch (Exception e) {
            // ignore error to prevent from influence on columnar query
            LOGGER.error("Failed to parse columnar version chain pruner string: " + prunerString, e);
        }

        return result;
    }

    public void reload(String newPrunerString) {
        if (this.prunerString != null && this.prunerString.equalsIgnoreCase(newPrunerString)) {
            // avoid unnecessary reload
            return;
        }
        this.prunerString = newPrunerString;
        this.prunerMap = parsePrunerString(newPrunerString);
    }

}
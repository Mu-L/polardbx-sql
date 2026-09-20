package com.alibaba.polardbx.gms.recyclebin;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PhyRecycleBinInfoAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhyRecycleBinInfoAccessor.class);

    private static final String ALL_COLUMNS =
        "`id`,`job_id`, `gmt_created`, `gmt_modified`, `storage_inst_id`, `origin_db_name`, `origin_tb_name`, `cur_db_name`, `cur_tb_name`, `type`, `status`, `extras`";

    private static final String ALL_VALUES = "(null, ?, now(), now(), ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_IGNORE_RECYCLE_BIN_INFO =
        "insert ignore into " + GmsSystemTables.PHY_RECYCLE_BIN_INFO + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String UPDATE_STATUS_BY_ID =
        "UPDATE " + GmsSystemTables.PHY_RECYCLE_BIN_INFO + " SET `status` = ? WHERE `id` = ?";

    private static final String UPDATE_STATUS_BY_STORAGE_TB =
        "UPDATE " + GmsSystemTables.PHY_RECYCLE_BIN_INFO
            + " SET `status` = ? WHERE `storage_inst_id`=? and `cur_tb_name` = ?";

    private static final String DELETE_FINISH_RECORD_BY_STORAGE_TB =
        "DELETE FROM " + GmsSystemTables.PHY_RECYCLE_BIN_INFO
            + " WHERE `storage_inst_id`=? and `cur_tb_name` = ? and status = 2";

    private static final String GET_ALL_UNDROP_TB_RECORD_BY_MINUTE =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.PHY_RECYCLE_BIN_INFO
            + " where status=1 and gmt_modified < DATE_SUB( NOW(), INTERVAL ? MINUTE ) limit 16384";

    // 插入一条记录
    public int insert(List<PhyRecycleBinInfoRecord> recordList) {
        try {
            if (CollectionUtils.isEmpty(recordList)) {
                return 0;
            }
            List<Map<Integer, ParameterContext>> paramsBatch =
                recordList.stream().map(e -> e.buildParams()).collect(Collectors.toList());
            DdlMetaLogUtil.logSql(INSERT_IGNORE_RECYCLE_BIN_INFO + " record count: " + recordList.size());
            int[] r = MetaDbUtil.insert(INSERT_IGNORE_RECYCLE_BIN_INFO, paramsBatch, connection);
            return Arrays.stream(r).sum();
        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.PHY_RECYCLE_BIN_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateByStorageAndTb(String storageId, String tbName, int status) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, storageId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tbName);

            DdlMetaLogUtil.logSql(UPDATE_STATUS_BY_STORAGE_TB, params);

            MetaDbUtil.update(UPDATE_STATUS_BY_STORAGE_TB, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update the system table " + GmsSystemTables.PHY_RECYCLE_BIN_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteFinishRecordByStorageAndTb(String storageId, String tbName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, storageId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tbName);

            DdlMetaLogUtil.logSql(DELETE_FINISH_RECORD_BY_STORAGE_TB, params);

            MetaDbUtil.update(DELETE_FINISH_RECORD_BY_STORAGE_TB, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update the system table " + GmsSystemTables.PHY_RECYCLE_BIN_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<PhyRecycleBinInfoRecord> getUnDropRecordByMinute(long minute) {
        try {

            List<PhyRecycleBinInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, minute);
            records =
                MetaDbUtil.query(GET_ALL_UNDROP_TB_RECORD_BY_MINUTE, params, PhyRecycleBinInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PHY_RECYCLE_BIN_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }
}

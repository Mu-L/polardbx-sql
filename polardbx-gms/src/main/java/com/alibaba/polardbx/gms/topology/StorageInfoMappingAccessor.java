package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class StorageInfoMappingAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(StorageInfoMappingAccessor.class);

    private static final String STORAGE_INFO_MAPPING_TABLE = GmsSystemTables.STORAGE_INFO_MAPPING;

    private static final String ALL_STORAGE_INFO_MAPPING_COLUMNS =
        "id, gmt_created, gmt_modified, upstream_inst_id, upstream_storage_inst_id, inst_id, storage_inst_id";

    private static final String SELECT_STORAGE_MAPPING_INFOS_BY_INST_ID =
        "select " + ALL_STORAGE_INFO_MAPPING_COLUMNS + " from `" + STORAGE_INFO_MAPPING_TABLE
            + "` where inst_id=? order by id";

    public List<StorageInfoMappingRecord> getStorageInfosByInstId(String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            return MetaDbUtil.query(SELECT_STORAGE_MAPPING_INFOS_BY_INST_ID,
                params, StorageInfoMappingRecord.class, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_MAPPING_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                STORAGE_INFO_MAPPING_TABLE, e.getMessage());
        }
    }
}

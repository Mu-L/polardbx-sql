package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoRecord;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;

import java.sql.Connection;

public class ExternalCatalogSyncAction implements ISyncAction {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalCatalogSyncAction.class);

    private String catalogName;
    private String action;

    public ExternalCatalogSyncAction() {
    }

    public ExternalCatalogSyncAction(String catalogName, String action) {
        this.catalogName = catalogName;
        this.action = action;
    }

    @Override
    public ResultCursor sync() {
        switch (action) {
        case "ADD":
        case "UPDATE":
            reloadCatalog();
            break;
        case "REMOVE":
            removeCatalog();
            break;
        case "REFRESH":
            refreshCatalog();
            break;
        default:
            LOGGER.warn("Unknown sync action: " + action);
            break;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private void reloadCatalog() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor();
            accessor.setConnection(conn);
            ExternalCatalogInfoRecord record = accessor.selectByName(catalogName);
            if (record != null) {
                ExternalCatalogManager.getInstance().register(record.toInfo());
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
        OptimizerContext.removeExternalSchemas(catalogName);
    }

    private void removeCatalog() {
        ExternalCatalogManager.getInstance().remove(catalogName);
        OptimizerContext.removeExternalSchemas(catalogName);
    }

    private void refreshCatalog() {
        OptimizerContext.removeExternalSchemas(catalogName);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}

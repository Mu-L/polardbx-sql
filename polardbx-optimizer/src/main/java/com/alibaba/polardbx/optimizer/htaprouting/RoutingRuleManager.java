package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleAccessor;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;
import java.util.List;

public class RoutingRuleManager extends AbstractLifecycle {
    public static final String ALL_USER = "%";

    public static final String INNER_RULE = "$inner$_";

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingRuleManager.class);

    private static final RoutingRuleManager INSTANCE = new RoutingRuleManager();

    private RoutingRuleClassifier classifier;

    public static RoutingRuleManager getInstance() {
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        if (!ConfigDataMode.isPolarDbX()) {
            return;
        }
        registerConfigListener();
        reload();
        FollowerReadRecord.getInstance().init();
    }

    public RoutingType determineRoutingType(ExecutionContext ec, String templateId, String sql) {
        HtapTrace.addTrace(ec.getHtapTrace(), "\ndetermine routing type:");
        RoutingRuleClassifier classifier = getClassifier();
        if (classifier == null) {
            HtapTrace.addTrace(ec.getHtapTrace(), "can't find classifier");
            return null;
        }
        return classifier.classify(ec, templateId, sql);
    }

    public RoutingRuleClassifier getClassifier() {
        return classifier;
    }

    public synchronized void reload() {
        List<RoutingRuleRecord> allRecords = null;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            RoutingRuleAccessor routingRuleAccessor = new RoutingRuleAccessor();
            routingRuleAccessor.setConnection(metaDbConn);
            allRecords = routingRuleAccessor.query(InstIdUtil.getInstId());
        } catch (Exception e) {
            LOGGER.error("failed to query routing rules", e);
        }
        this.classifier = RoutingRuleClassifier.build(allRecords);
    }

    private void registerConfigListener() {
        try {
            String dataId = MetaDbDataIdBuilder.getRoutingRuleDataId(InstIdUtil.getInstId());
            MetaDbConfigManager.getInstance().register(dataId, null);
            RoutingRuleManager.RoutingTypeListener listener = new RoutingRuleManager.RoutingTypeListener();
            MetaDbConfigManager.getInstance().bindListener(dataId, listener);
        } catch (Exception ignore) {
        }
    }

    private class RoutingTypeListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            LOGGER.info(String.format("start reload routing rules , newOpVersion: %d", newOpVersion));
            reload();
        }
    }
}

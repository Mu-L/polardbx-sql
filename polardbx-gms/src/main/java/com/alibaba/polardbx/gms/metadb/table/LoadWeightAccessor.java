package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.ArrayList;
import java.util.List;

public class LoadWeightAccessor extends AbstractAccessor {
    private static final String QUERY_RECORD = "select id, inst_id, node, load_weight, gmt_created from load_weight";

    public List<LoadWeightRecord> query() {
        try {
            return MetaDbUtil.query(QUERY_RECORD, LoadWeightRecord.class, connection);
        } catch (Exception e) {
            // return a empty list to make sure cn can startup gracefully
            return new ArrayList<>();
        }
    }
}

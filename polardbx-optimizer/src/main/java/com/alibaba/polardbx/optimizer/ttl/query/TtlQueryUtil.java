package com.alibaba.polardbx.optimizer.ttl.query;

import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import org.apache.calcite.rel.RelNode;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class TtlQueryUtil {

    public static ExecutionPlan replaceTtlQueryBoundaryWithLiteral(ExecutionPlan executionPlan, ExecutionContext ec) {
        RelNode plan = executionPlan.getPlan();
        TtlQueryBoundaryReplacer ttlQueryBoundaryReplacer = new TtlQueryBoundaryReplacer(ec);
        RelNode newPlan = plan.accept(ttlQueryBoundaryReplacer);
        if (newPlan != plan) {
            return executionPlan.copy(newPlan);
        }
        return executionPlan;
    }

    public static String getTtlQueryBoundary(ExecutionContext ec, String schema, String table) {
        TableMeta tableMeta = ec.getSchemaManager(schema).getTable(table);
        TtlDefinitionInfo ttlDefinitionInfo = tableMeta.getTtlDefinitionInfo();
        if (ttlDefinitionInfo == null) {
            return null;
        }
        ExtraFieldJSON extraFieldJSON = ttlDefinitionInfo.getTtlInfoRecord().getExtra();
        if (extraFieldJSON == null) {
            return null;
        }
        String arcBound = extraFieldJSON.getArcBound();
        if (arcBound == null) {
            return null;
        }
        if (ttlDefinitionInfo.getTtlColMeta(ec).getDataType().getClass() == TimestampType.class) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String ttlColTimezone = ttlDefinitionInfo.getTtlInfoRecord().getTtlTimezone();
            OffsetDateTime offsetDateTime =
                OffsetDateTime.of(LocalDateTime.parse(arcBound, dateTimeFormatter), ZoneOffset.of(ttlColTimezone));

            TimeZone sessionTimeZone = ec.getTimeZone().getTimeZone();
            ZonedDateTime zonedDateTime = offsetDateTime.atZoneSameInstant(ZoneId.of(sessionTimeZone.getID()));
            return zonedDateTime.format(dateTimeFormatter);
        }
        return arcBound;
    }

    public static Map<String, String> getRefColQueryBoundary(TableMeta tableMeta) {
        TtlDefinitionInfo ttlDefinitionInfo = tableMeta.getTtlDefinitionInfo();
        if (ttlDefinitionInfo == null) {
            return null;
        }
        ExtraFieldJSON extraFieldJSON = ttlDefinitionInfo.getTtlInfoRecord().getExtra();
        if (extraFieldJSON == null) {
            return null;
        }
        if (extraFieldJSON.getTtlRefColList() == null || extraFieldJSON.getTtlRefColValueList() == null) {
            return null;
        }
        Map<String, String> refColQueryBoundary = new HashMap<>();
        for (int i = 0;
             i < Math.min(extraFieldJSON.getTtlRefColList().size(), extraFieldJSON.getTtlRefColValueList().size());
             i++) {
            refColQueryBoundary.put(extraFieldJSON.getTtlRefColList().get(i),
                extraFieldJSON.getTtlRefColValueList().get(i));
        }
        return refColQueryBoundary;
    }

}

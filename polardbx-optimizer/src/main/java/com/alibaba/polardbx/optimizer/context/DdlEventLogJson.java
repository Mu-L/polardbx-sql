package com.alibaba.polardbx.optimizer.context;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.TStringUtil;
import lombok.Getter;
import lombok.Setter;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author wumu
 */
@Getter
@Setter
public class DdlEventLogJson {
    public long jobId;

    public String schemaName;

    public String objectName;

    public String state;

    public String type;

    public String ddlStmt;

    public String jobFactoryName;

    public String taskName;

    public String errorMessage;

    public int ddlStmtLength;

    public String ddlStmtDigest;

    // Change context:
    // - Before: the event log JSON only carried ddlStmt itself; downstream alert aggregation
    //   (SLS fire_results[0]) silently truncates long fields (~1024 bytes) and consumers had no
    //   way to tell an incomplete ddlStmt from the original statement.
    // - Path impact: all DDL engine events built via create() (DDL_PAUSED_NEW /
    //   DDL_ROLLBACK_COMPLETED / DDL_COMPLETED in DdlEngineDagExecutor) gain two additive JSON
    //   fields; fromJson stays compatible because absent fields deserialize to defaults. The
    //   ddlStmt content and its non-truncated output behavior are unchanged.
    // - Capability regression: None. One length computation and one MD5 digest per DDL event,
    //   negligible cost; no truncation or compatibility fallback is removed.
    public static DdlEventLogJson create(DdlContext ddlContext) {
        DdlEventLogJson ddlEventLogJson = new DdlEventLogJson();
        ddlEventLogJson.jobId = ddlContext.getJobId();
        ddlEventLogJson.schemaName = ddlContext.getSchemaName();
        ddlEventLogJson.objectName = ddlContext.getObjectName();
        ddlEventLogJson.state = ddlContext.getState().name();
        ddlEventLogJson.type = ddlContext.getDdlType().name();
        ddlEventLogJson.jobFactoryName = ddlContext.getDdlJobFactoryName();
        ddlEventLogJson.ddlStmt = TStringUtil.quoteString(ddlContext.getDdlStmt());
        ddlEventLogJson.ddlStmtLength = ddlContext.getDdlStmt().length();
        ddlEventLogJson.ddlStmtDigest = md5Hex(ddlContext.getDdlStmt());
        return ddlEventLogJson;
    }

    private static String md5Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("MD5 algorithm not available", e);
        }
    }

    public static DdlEventLogJson create(DdlContext ddlContext, String taskName, String errorMessage) {
        DdlEventLogJson ddlEventLogJson = create(ddlContext);
        ddlEventLogJson.taskName = taskName;
        ddlEventLogJson.errorMessage = errorMessage;
        return ddlEventLogJson;
    }

    public static DdlEventLogJson fromJson(String json) {
        return JSON.parseObject(json, DdlEventLogJson.class);
    }

    public static String toJson(DdlEventLogJson obj) {
        if (obj == null) {
            return "";
        }
        return JSON.toJSONString(obj);
    }
}

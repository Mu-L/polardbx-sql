package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.util.StringUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

/**
 * @author chenghui.lch
 */
public class TtlColFuncExprInfo {

    /**
     * The func expr for ttl-col from int-type to datetime-type
     */
    /**
     * The ttl col func expr string replaced ttl-col as dynamic params
     * which is used to decode the int-type ttl_col into iso-formated datetime string
     */
    protected String normalizedTtlColFuncExprStr = null;

    /**
     * Flag label if the int-type ttl_col is treated as unix_timestamp int
     */
    protected boolean treatTtlColAsUnixTimestampSeconds = false;
    /**
     * Flag label if the int-type ttl_col is treated as millis timestamp int
     */
    protected boolean treatTtlColAsUnixTimestampMillSeconds = false;
    protected boolean treatTtlColAsToDaysNumber = false;

    /**
     * Label if ttl_col use ttlColEncoder/ttlColDecoder
     */
    protected boolean useTtlColEncoding = false;

    protected String ttlColNameName = null;

    protected String ttlColEncoderStr = null;
    protected String ttlColFullEncoderStr = null;

    protected String ttlColDecoderStr = null;
    protected String ttlColFullDecoderStr = null;

    /**
     * The query expr template for validating encoder and decoder
     */
    protected String ttlColValidationExprTemplate = null;

    /**
     * The string of the ttlColEncoder expr with using ttl-col as input,like
     * <pre>
     *     ttl_decoder = from_unixtime( ? / 1000),
     *     the ttlColDecoderUsingTtlColAsInputExpr is
     *              from_unixtime( `ttl_col` / 1000 )
     * </pre>
     */
    protected String ttlColDecoderUsingTtlColAsInputExpr = null;

    protected TtlColFuncExprInfo() {
    }

    public static TtlColFuncExprInfo buildTtlColFuncExprInfoByTtlColAst(SqlNode ttlColNodeAst,
                                                                        String ttlColEncoder,
                                                                        String ttlColDecoder) {
        boolean useFuncExprDef = (ttlColNodeAst instanceof SqlBasicCall);
        boolean useTtlColExprEncoding = !StringUtils.isEmpty(ttlColEncoder) && !StringUtils.isEmpty(ttlColDecoder);

        if (!useFuncExprDef && !useTtlColExprEncoding) {
            return null;
        }

        TtlColFuncExprInfo funcExprInfo = new TtlColFuncExprInfo();
        TtlUtil.TtlColumnFinder ttlColumnFinder = new TtlUtil.TtlColumnFinder();
        ttlColumnFinder.find(ttlColNodeAst);
        String ttlColName = SQLUtils.normalizeNoTrim(ttlColumnFinder.getTtlColumn().getLastName());
        if (useFuncExprDef) {

//            TtlUtil.TtlColumnAsSqlDynamicParamReplacer
//                dynamicNodeReplacer = new TtlUtil.TtlColumnAsSqlDynamicParamReplacer();
//            SqlNode ttlFuncExprWithDynamicParamInput = ttlColFuncExpr.accept(dynamicNodeReplacer);
//            String ttlFuncExprStrWithDynamicParamInput = ttlFuncExprWithDynamicParamInput.toString();

            boolean ttlColUseFromUnixTimeWithoutDiv = ttlColumnFinder.ttlColUseFromUnixTimeFuncWithoutDiv();
            boolean ttlColUseFromUnixTimeWithDiv = ttlColumnFinder.ttlColUseFromUnixTimeFuncWithDiv();
            boolean ttlColUseFromDays = ttlColumnFinder.ttlColUseFromDays();

//            funcExprInfo.setNormalizedTtlColFuncExprStr(ttlFuncExprStrWithDynamicParamInput);
//            funcExprInfo.setTreatTtlColAsUnixTimestampSeconds(ttlColUseFromUnixTimeWithoutDiv);
//            funcExprInfo.setTreatTtlColAsUnixTimestampMillSeconds(ttlColUseFromUnixTimeWithDiv);
//            funcExprInfo.setTreatTtlColAsToDaysNumber(ttlColUseFromDays);

            if (ttlColUseFromUnixTimeWithoutDiv) {
                ttlColEncoder = "UNIX_TIMESTAMP(?)";
                ttlColDecoder = "FROM_UNIXTIME(?)";
            } else if (ttlColUseFromUnixTimeWithDiv) {
                ttlColEncoder = "UNIX_TIMESTAMP(?) * 1000";
                ttlColDecoder = "FROM_UNIXTIME(? / 1000)";
            } else if (ttlColUseFromDays) {
                ttlColEncoder = "TO_DAYS(?)";
                ttlColDecoder = "FROM_DAYS(?)";
            }
        }

        boolean isFullEncoderExpr = false;
        String ttlColFullEncoderExpr = TtlColFuncExprInfo.buildFullEncoderExpr(ttlColEncoder, isFullEncoderExpr);

        boolean isFullDecoderExpr = false;
        String ttlColFullDecoderExpr = TtlColFuncExprInfo.buildFullDecoderExpr(ttlColDecoder, isFullDecoderExpr);
        String ttlColFullDecoderExprWithTtlColAsInput =
            TtlColFuncExprInfo.buildFullDecoderExprWithTtlColAsInput(ttlColFullDecoderExpr, ttlColName);

        String validatorQueryExprTemp =
            TtlColFuncExprInfo.buildValidationExprTemplate(ttlColFullEncoderExpr, ttlColFullDecoderExpr);

        funcExprInfo.setUseTtlColEncoding(useTtlColExprEncoding);
        funcExprInfo.setTtlColNameName(ttlColName);

        funcExprInfo.setTtlColEncoderStr(ttlColEncoder);
        funcExprInfo.setTtlColFullEncoderStr(ttlColFullEncoderExpr);

        funcExprInfo.setTtlColDecoderStr(ttlColDecoder);
        funcExprInfo.setTtlColFullDecoderStr(ttlColFullDecoderExpr);
        funcExprInfo.setTtlColDecoderUsingTtlColAsInputExpr(ttlColFullDecoderExprWithTtlColAsInput);

        funcExprInfo.setTtlColValidationExprTemplate(validatorQueryExprTemp);

        return funcExprInfo;
    }

    public TtlColFuncExprInfo copy() {
        TtlColFuncExprInfo newTtlColFuncExprInfo = new TtlColFuncExprInfo();
//        newTtlColFuncExprInfo.setNormalizedTtlColFuncExprStr(normalizedTtlColFuncExprStr);
//        newTtlColFuncExprInfo.setTreatTtlColAsUnixTimestampMillSeconds(treatTtlColAsUnixTimestampMillSeconds);
//        newTtlColFuncExprInfo.setTreatTtlColAsUnixTimestampSeconds(treatTtlColAsUnixTimestampSeconds);
//        newTtlColFuncExprInfo.setTreatTtlColAsToDaysNumber(treatTtlColAsToDaysNumber);

        newTtlColFuncExprInfo.setUseTtlColEncoding(useTtlColEncoding);
        newTtlColFuncExprInfo.setTtlColNameName(ttlColNameName);

        newTtlColFuncExprInfo.setTtlColEncoderStr(ttlColEncoderStr);
        newTtlColFuncExprInfo.setTtlColFullEncoderStr(ttlColFullEncoderStr);

        newTtlColFuncExprInfo.setTtlColDecoderStr(ttlColDecoderStr);
        newTtlColFuncExprInfo.setTtlColFullDecoderStr(ttlColFullDecoderStr);
        newTtlColFuncExprInfo.setTtlColDecoderUsingTtlColAsInputExpr(ttlColDecoderUsingTtlColAsInputExpr);

        newTtlColFuncExprInfo.setTtlColValidationExprTemplate(ttlColValidationExprTemplate);


        return newTtlColFuncExprInfo;
    }

    protected static String buildFullDecoderExprWithTtlColAsInput(String ttlColFullDecoderStr,
                                                                  String ttlColName) {
        String fullDecoderExprStr = ttlColFullDecoderStr;
        if (fullDecoderExprStr == null) {
            return null;
        }
        SQLExpr fullDecoderExpr = TtlUtil.parseExprString(fullDecoderExprStr);
        String ttlColNameStr = String.format("`%s`", ttlColName);
        String fullDecoderExprStrWithTtlColAsInputVal =
            TtlUtil.replaceParamsAndBuildExprSql(ttlColNameStr, fullDecoderExpr);

        // fullDecoderExprStr = DATE_FORMAT( decoder_expr( CAST( `ttl_col` AS CHAR ), param1, ... ), '%Y-%m-%d %H:%i:%s' )
        return fullDecoderExprStrWithTtlColAsInputVal;
    }

    protected static String buildFullDecoderExpr(String ttlColDecoderStr,
                                                 boolean isFullExpr) {
        String fullDecoderExprStr = null;
        SQLExpr ttlColDecoderExpr = TtlUtil.parseExprString(ttlColDecoderStr);
        if (ttlColDecoderExpr == null) {
            return fullDecoderExprStr;
        }
        if (isFullExpr) {
            return fullDecoderExprStr;
        }
        String caseToCharExprStr = "CAST( ? AS CHAR )";
        String isoDatetimeFormatExprStr = "DATE_FORMAT( ?, '%Y-%m-%d %H:%i:%s' )";

        // fullDecoderExprStr = DATE_FORMAT( CAST( ? AS CHAR ), '%Y-%m-%d %H:%i:%s' )
        SQLExpr isoDatetimeFormatExpr = TtlUtil.parseExprString(isoDatetimeFormatExprStr);
        String fullDecoderExprTempStr = TtlUtil.replaceParamsAndBuildExprSql(caseToCharExprStr, isoDatetimeFormatExpr);

        // fullDecoderExprStr = DATE_FORMAT( CAST( decoder_expr( ?, param1, ... ) AS CHAR ), '%Y-%m-%d %H:%i:%s' )
        SQLExpr fullDecoderExprTempAst = TtlUtil.parseExprString(fullDecoderExprTempStr);
        String ttlColDecoderStrVal = TtlUtil.replaceParamsAndBuildExprSql(ttlColDecoderStr, fullDecoderExprTempAst);

        return ttlColDecoderStrVal;
    }

    protected static String buildFullEncoderExpr(String ttlColEncoderStr,
                                                 boolean isFullExpr) {
        String fullEncoderExprStr = null;
        SQLExpr ttlColEncoderExpr = TtlUtil.parseExprString(ttlColEncoderStr);
        if (ttlColEncoderExpr == null) {
            return fullEncoderExprStr;
        }
        if (isFullExpr) {
            return fullEncoderExprStr;
        }
        // ttlColDecoderStrVal = encoder_expr( STR_TO_DATE( ?, '%Y-%m-%d %H:%i:%s' ), param1, ... )
        String strToDateExprStr = "STR_TO_DATE( ?, '%Y-%m-%d %H:%i:%s' )";
        String ttlColDecoderStrVal = TtlUtil.replaceParamsAndBuildExprSql(strToDateExprStr, ttlColEncoderExpr);

        // fullEncoderExprStr = CAST( encoder_expr( STR_TO_DATE( ?, '%Y-%m-%d %H:%i:%s' ), param1, ... ) AS SIGNED )
//        String caseToUnsignedExprStr = "CAST( ? AS SIGNED )";
//        SQLExpr caseToUnsignedExpr = TtlUtil.parseExprString(caseToUnsignedExprStr);
//        fullEncoderExprStr = TtlUtil.replaceParamsAndBuildExprSql(ttlColDecoderStrVal, caseToUnsignedExpr);

        fullEncoderExprStr = ttlColDecoderStrVal;
        return fullEncoderExprStr;
    }

    protected static String buildValidationExprTemplate(String fullEncoderExprStr,
                                                        String fullDecoderExpStr) {

        String validatorDatetimeStr = "?";
        SQLExpr fullDecoderExpr = TtlUtil.parseExprString(fullDecoderExpStr);
        SQLExpr fullEncoderExpr = TtlUtil.parseExprString(fullEncoderExprStr);

        String fullEncoderExprVal = TtlUtil.replaceParamsAndBuildExprSql(validatorDatetimeStr, fullEncoderExpr);
        String validationQueryExprValTemp = TtlUtil.replaceParamsAndBuildExprSql(fullEncoderExprVal, fullDecoderExpr);

        // fullEncoderExprStr = CAST( encoder_expr( STR_TO_DATE( ?, '%Y-%m-%d %H:%i:%s' ), param1, ... ) AS UNSIGNED )
        // fullDecoderExprStr = DATE_FORMAT( decoder_expr( CAST( ? AS CHAR ), param1, ... ), '%Y-%m-%d %H:%i:%s' )

        // validationQueryExprVal = DATE_FORMAT(
        //      decoder_expr (
        //              CAST(
        //                  CAST( encoder_expr( STR_TO_DATE( '1970-01-01 12:34:56', '%Y-%m-%d %H:%i:%s' ), param1, ... ) AS UNSIGNED )
        //                  AS CHAR
        //              ),
        //              param1,
        //              ...
        //      ),
        //      '%Y-%m-%d %H:%i:%s'
        // )
        //


        // validationQueryExprVal = DATE_FORMAT(
        //      decoder_expr (
        //              CAST(
        //                  encoder_expr( STR_TO_DATE( '1970-01-01 12:34:56', '%Y-%m-%d %H:%i:%s' ), param1, ... )
        //                  AS UNSIGNED
        //              ),
        //              param1,
        //              ...
        //      ),
        //      '%Y-%m-%d %H:%i:%s'
        // )
        //
        return validationQueryExprValTemp;
    }

    /**
     * Get the decode expr of ttl_col, like from_unixtime(?)
     */
    public String getNormalizedTtlColFuncExprStr() {
        return normalizedTtlColFuncExprStr;
    }

    public void setNormalizedTtlColFuncExprStr(String normalizedTtlColFuncExprStr) {
        this.normalizedTtlColFuncExprStr = normalizedTtlColFuncExprStr;
    }

    public boolean isTreatTtlColAsUnixTimestampSeconds() {
        return treatTtlColAsUnixTimestampSeconds;
    }

    public void setTreatTtlColAsUnixTimestampSeconds(boolean treatTtlColAsUnixTimestampSeconds) {
        this.treatTtlColAsUnixTimestampSeconds = treatTtlColAsUnixTimestampSeconds;
    }

    public boolean isTreatTtlColAsUnixTimestampMillSeconds() {
        return treatTtlColAsUnixTimestampMillSeconds;
    }

    public void setTreatTtlColAsUnixTimestampMillSeconds(boolean ttlColUseFromUnixTimeWithDiv) {
        this.treatTtlColAsUnixTimestampMillSeconds = ttlColUseFromUnixTimeWithDiv;
    }

    public boolean isTreatTtlColAsToDaysNumber() {
        return treatTtlColAsToDaysNumber;
    }

    public void setTreatTtlColAsToDaysNumber(boolean treatTtlColAsToDaysNumber) {
        this.treatTtlColAsToDaysNumber = treatTtlColAsToDaysNumber;
    }

    public String getTtlColEncoderStr() {
        return ttlColEncoderStr;
    }

    public void setTtlColEncoderStr(String ttlColEncoderStr) {
        this.ttlColEncoderStr = ttlColEncoderStr;
    }

    public String getTtlColDecoderStr() {
        return ttlColDecoderStr;
    }

    public void setTtlColDecoderStr(String ttlColDecoderStr) {
        this.ttlColDecoderStr = ttlColDecoderStr;
    }

    public boolean isUseTtlColEncoding() {
        return useTtlColEncoding;
    }

    public void setUseTtlColEncoding(boolean useTtlColEncoding) {
        this.useTtlColEncoding = useTtlColEncoding;
    }

    public String getTtlColDecoderUsingTtlColAsInputExpr() {
        return ttlColDecoderUsingTtlColAsInputExpr;
    }

    public void setTtlColDecoderUsingTtlColAsInputExpr(String ttlColDecoderUsingTtlColAsInputExpr) {
        this.ttlColDecoderUsingTtlColAsInputExpr = ttlColDecoderUsingTtlColAsInputExpr;
    }

    public String getTtlColNameName() {
        return ttlColNameName;
    }

    public void setTtlColNameName(String ttlColNameName) {
        this.ttlColNameName = ttlColNameName;
    }

    public String getTtlColFullEncoderStr() {
        return ttlColFullEncoderStr;
    }

    public void setTtlColFullEncoderStr(String ttlColFullEncoderStr) {
        this.ttlColFullEncoderStr = ttlColFullEncoderStr;
    }

    public String getTtlColFullDecoderStr() {
        return ttlColFullDecoderStr;
    }

    public void setTtlColFullDecoderStr(String ttlColFullDecoderStr) {
        this.ttlColFullDecoderStr = ttlColFullDecoderStr;
    }

    public String getTtlColValidationExprTemplate() {
        return ttlColValidationExprTemplate;
    }

    public void setTtlColValidationExprTemplate(String ttlColValidationExprTemplate) {
        this.ttlColValidationExprTemplate = ttlColValidationExprTemplate;
    }
}

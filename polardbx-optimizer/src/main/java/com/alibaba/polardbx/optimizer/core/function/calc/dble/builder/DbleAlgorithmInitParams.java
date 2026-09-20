package com.alibaba.polardbx.optimizer.core.function.calc.dble.builder;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.support.json.JSONUtils;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class DbleAlgorithmInitParams {

    public static final String FIELD_KEY_PARTITION_COUNT = "partitionCount";
    public static final String FIELD_KEY_PARTITION_LENGTH = "partitionLength";
    public static final String FIELD_KEY_HASH_SLICE = "hashSlice";
    public static final String FIELD_KEY_DEFAULT_NODE = "defaultNode";
    public static final String FIELD_KEY_TYPE = "type";
    public static final String FIELD_KEY_MAPPINGS = "mappings";
    public static final String FIELD_KEY_MAP_FILE = "mapFile";
    public static final String FIELD_KEY_PATTERN_VALUE = "patternValue";
    public static final String FIELD_KEY_PARTITION_DATE_FORMAT = "dateFormat";
    public static final String FIELD_KEY_BEGIN_DATE = "sBeginDate";
    public static final String FIELD_KEY_END_DATE = "sEndDate";
    public static final String FIELD_KEY_PARTITION_DAY = "partitionDay";
    public static final String FIELD_KEY_SPARTTION_DAY = "sPartionDay";

    public static final String FIELD_KEY_IS_DIGIT_CHECK = "isDigitCheck";
    public static final String FIELD_KEY_LEFT_PADDING_CHAR = "leftPaddingChar";
    public static final String FIELD_KEY_SHARDING_VALUE_MAX_LENGTH = "shardingValueMaxLength";

    protected BuildParams buildParams;

    protected DbleAlgorithmType algorithmType;
    protected Map<String, Object> initParamsJson;


    public DbleAlgorithmInitParams() {
    }

    public static Map<String, Object> buildMappingsFromJsonParams(String initParamsJsonText) {
        Map<String, Object> initParamsMappings = (Map<String, Object>) JSONUtils.parse(initParamsJsonText);
        return initParamsMappings;
    }

    public static DbleAlgorithmInitParams buildFromJson(String algorithmName, String initParamsJsonText) {

        try {
            Map<String, Object> fullParamsMappings = buildMappingsFromJsonParams(initParamsJsonText);

            Map<String, Object> initParamsJson = fullParamsMappings;
            DbleAlgorithmType algorithmTypeVal = DbleAlgorithmType.getDbleAlgorithmTypeByName(algorithmName);

            BuildParams buildParamsVal = new BuildParams();
            DbleAlgorithmInitParams initParams = new DbleAlgorithmInitParams();

            initParams.setAlgorithmType(algorithmTypeVal);
            initParams.setBuildParams(buildParamsVal);
            initParams.setInitParamsJson(fullParamsMappings);

//            buildParamsVal.setActualPartitionCount();

            buildParamsVal.setAlgorithmType(algorithmTypeVal.getAlgorithmIndex());
            if (algorithmTypeVal == DbleAlgorithmType.DBLE_PARTITION_BY_LONG) {

//                String count = params.getPartitionCount();
//                String length = params.getPartitionLength();
//                PartitionByLong routeFunc = new PartitionByLong();
//                routeFunc.setPartitionCount(count);
//                routeFunc.setPartitionLength(length);
//                routeFunc.init();

                String partitionCountVal = (String) initParamsJson.get(FIELD_KEY_PARTITION_COUNT);
                String partitionLengthVal =  (String) initParamsJson.get(FIELD_KEY_PARTITION_LENGTH);
                buildParamsVal.setPartitionCount(partitionCountVal);
                buildParamsVal.setPartitionLength(partitionLengthVal);

            } else if (algorithmTypeVal == DbleAlgorithmType.DBLE_PARTITION_BY_STRING) {

//                String count = params.getPartitionCount();
//                String length = params.getPartitionLength();
//                String hashSlice = params.getHashSlice();
//                PartitionByString routeFunc = new PartitionByString();
//                routeFunc.setPartitionCount(count);
//                routeFunc.setPartitionLength(length);
//                routeFunc.setHashSlice(hashSlice);
//                routeFunc.init();

                String partitionCountVal = (String) initParamsJson.get(FIELD_KEY_PARTITION_COUNT);
                String partitionLengthVal = (String) initParamsJson.get(FIELD_KEY_PARTITION_LENGTH);
                String hashSlice = (String) initParamsJson.get(FIELD_KEY_HASH_SLICE);
                buildParamsVal.setPartitionCount(partitionCountVal);
                buildParamsVal.setPartitionLength(partitionLengthVal);
                buildParamsVal.setHashSlice(hashSlice);

            } else if (algorithmTypeVal == DbleAlgorithmType.DBLE_PARTITION_BY_FILE_MAP) {

//                int defaultNode = params.getDefaultNode();
//                int type = params.getType();
//                List<Pair<String,Integer>> mappings = params.getPartitionMappings();
//                PartitionByFileMap routeFunc = new PartitionByFileMap();
//                routeFunc.setDefaultNode(defaultNode);
//                routeFunc.setType(type);
//                routeFunc.setPartitionMappings(mappings);
//                routeFunc.init();

                String defaultNode = (String) initParamsJson.get(FIELD_KEY_DEFAULT_NODE);
                if (defaultNode == null) {
                    defaultNode = "-1";
                }

                String type = (String) initParamsJson.get(FIELD_KEY_TYPE);
                if (type == null) {
                    type = "0";
                }
                String mapFileString = (String) initParamsJson.get(FIELD_KEY_MAP_FILE);
                List<Pair<String, Integer>> mappingList = new ArrayList<>();
                List<Object> mappingArray = (List<Object>) initParamsJson.get(FIELD_KEY_MAPPINGS);
                if (mappingArray != null) {
                    mappingList = toMappingList(mappingArray);
                }

                buildParamsVal.setDefaultNode(Integer.valueOf(defaultNode));
                buildParamsVal.setType(Integer.valueOf(type));
                buildParamsVal.setPartitionMappings(mappingList);
                buildParamsVal.setMapFileString(mapFileString);

            } else if (algorithmTypeVal == DbleAlgorithmType.DBLE_AUTO_PARTITION_BY_LONG) {

//                int defaultNode = params.getDefaultNode();
//                List<Pair<String,Integer>> mappings = params.getPartitionMappings();
//                AutoPartitionByLong routeFunc = new AutoPartitionByLong();
//                routeFunc.setDefaultNode(defaultNode);
//                routeFunc.setPartitionMappings(mappings);
//                routeFunc.init();

                String defaultNode = (String) initParamsJson.get(FIELD_KEY_DEFAULT_NODE);
                if (defaultNode == null) {
                    defaultNode = "-1";
                }
                String mapFileString = (String) initParamsJson.get(FIELD_KEY_MAP_FILE);
                List<Pair<String, Integer>> mappingList = new ArrayList<>();
                List<Object> mappingArray = (List<Object>) initParamsJson.get(FIELD_KEY_MAPPINGS);
                if (mappingArray != null) {
                    mappingList = toMappingList(mappingArray);
                }

                buildParamsVal.setDefaultNode(Integer.valueOf(defaultNode));
                buildParamsVal.setPartitionMappings(mappingList);
                buildParamsVal.setMapFileString(mapFileString);

            } else if (algorithmTypeVal == DbleAlgorithmType.DBLE_PARTITION_BY_PATTERN) {

//                int defaultNode = params.getDefaultNode();
//                int patternValue = params.getPartitionValue();
//                List<Pair<String,Integer>> mappings = params.getPartitionMappings();
//                PartitionByPattern routeFunc = new PartitionByPattern();
//                routeFunc.setDefaultNode(defaultNode);
//                routeFunc.setPatternValue(patternValue);
//                routeFunc.setPartitionMappings(mappings);
//                routeFunc.init();

                String defaultNode = (String) initParamsJson.get(FIELD_KEY_DEFAULT_NODE);
                if (defaultNode == null) {
                    defaultNode = "-1";
                }
                String patternValue = (String) initParamsJson.get(FIELD_KEY_PATTERN_VALUE);
                if (patternValue == null) {
                    patternValue = "1024";
                }

                String mapFileString = (String) initParamsJson.get(FIELD_KEY_MAP_FILE);
                List<Pair<String, Integer>> mappingList = new ArrayList<>();
                List<Object> mappingArray = (List<Object>) initParamsJson.get(FIELD_KEY_MAPPINGS);
                if (mappingArray != null) {
                    mappingList = toMappingList(mappingArray);
                }
                buildParamsVal.setDefaultNode(Integer.valueOf(defaultNode));
                buildParamsVal.setPatternValue(Integer.valueOf(patternValue));
                buildParamsVal.setPartitionMappings(mappingList);
                buildParamsVal.setMapFileString(mapFileString);


            } else if (algorithmTypeVal == DbleAlgorithmType.DBLE_PARTITION_BY_DATE) {

//                int defaultNode = params.getDefaultNode();
//                String dateFormat = params.getDateFormat();
//                String sBeginDate = params.getsBeginDate();
//                String sEndDate = params.getsEndDate();
//                String sPartionDay = params.getsPartitionDay();
//                PartitionByDate routeFunc = new PartitionByDate();
//                routeFunc.setDefaultNode(defaultNode);
//                routeFunc.setDateFormat(dateFormat);
//                routeFunc.setsBeginDate(sBeginDate);
//                routeFunc.setsEndDate(sEndDate);
//                routeFunc.setsPartionDay(sPartionDay);
//                routeFunc.init();

                String defaultNode = (String) initParamsJson.get(FIELD_KEY_DEFAULT_NODE);
                if (defaultNode == null) {
                    defaultNode = "-1";
                }
                String partitionDateFormat = (String) initParamsJson.get(FIELD_KEY_PARTITION_DATE_FORMAT);
                String sBeginDate = (String) initParamsJson.get(FIELD_KEY_BEGIN_DATE);
                String sEndDate = (String) initParamsJson.get(FIELD_KEY_END_DATE);
                String sPartitionDay = (String) initParamsJson.get(FIELD_KEY_SPARTTION_DAY);
                if (sPartitionDay == null) {
                    sPartitionDay = (String) initParamsJson.get(FIELD_KEY_PARTITION_DAY);
                }

                buildParamsVal.setDefaultNode(Integer.valueOf(defaultNode));
                buildParamsVal.setDateFormat(partitionDateFormat);
                buildParamsVal.setsBeginDate(sBeginDate);
                buildParamsVal.setsEndDate(sEndDate);
                buildParamsVal.setsPartitionDay(sPartitionDay);

            } else if (algorithmTypeVal == DbleAlgorithmType.DBLE_PARTITION_BY_JUMP_CONSISTENT_HASH) {

//                int count = Integer.valueOf(params.getPartitionCount());
//                String hashSlice = params.getHashSlice();
//                PartitionByJumpConsistentHash routeFunc = new PartitionByJumpConsistentHash();
//                routeFunc.setPartitionCount(count);
//                routeFunc.setHashSlice(hashSlice);
//                routeFunc.init();

                String partitionCountVal = (String) initParamsJson.get(FIELD_KEY_PARTITION_COUNT);
                String hashSlice = (String) initParamsJson.get(FIELD_KEY_HASH_SLICE);
                buildParamsVal.setPartitionCount(partitionCountVal);
                buildParamsVal.setPartitionLength(hashSlice);
                buildParamsVal.setHashSlice(hashSlice);

            } else if (algorithmTypeVal == DbleAlgorithmType.DBLE_PARTITION_BY_GH_UNIT_MURMUR_HASH) {

//                PartitionByUnitMurmurHash routeFunc = new PartitionByUnitMurmurHash();
//                routeFunc.setPartitionCount(params.getPartitionCount());
//                // default isDigitCheck=1
//                routeFunc.setIsDigitCheck(params.getIsDigitCheck());
//                // default leftPaddingChar=NO
//                routeFunc.setLeftPaddingChar(params.getLeftPaddingChar());
//                // default shardingValueMaxLength=15
//                routeFunc.setShardingValueMaxLength(params.getShardingValueMaxLength());
//                routeFunc.init();

                String partitionCountVal = (String) initParamsJson.get(FIELD_KEY_PARTITION_COUNT);
                String isDigitCheck = (String) initParamsJson.get(FIELD_KEY_IS_DIGIT_CHECK);
                String leftPaddingChar = (String) initParamsJson.get(FIELD_KEY_LEFT_PADDING_CHAR);
                String shardingValueMaxLength = (String) initParamsJson.get(FIELD_KEY_SHARDING_VALUE_MAX_LENGTH);
                buildParamsVal.setPartitionCount(partitionCountVal);
                buildParamsVal.setIsDigitCheck(isDigitCheck);
                buildParamsVal.setLeftPaddingChar(leftPaddingChar);
                buildParamsVal.setShardingValueMaxLength(shardingValueMaxLength);

            }
            return initParams;
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format("Failed to create dble algorithm by init params, the params is [ %s ]", initParamsJsonText), e);
        }
    }

    public DbleAlgorithmType getAlgorithmType() {
        return algorithmType;
    }

    public void setAlgorithmType(DbleAlgorithmType algorithmType) {
        this.algorithmType = algorithmType;
    }

    public BuildParams getBuildParams() {
        return buildParams;
    }

    public void setBuildParams(BuildParams buildParams) {
        this.buildParams = buildParams;
    }

    public Map<String, Object> getInitParamsJson() {
        return initParamsJson;
    }

    public void setInitParamsJson(Map<String, Object> initParamsMappings) {
        this.initParamsJson = initParamsMappings;
    }

    private static List<Pair<String, Integer>> toMappingList (List<Object> array) {
        List<Pair<String, Integer>> rs = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            String itemStr = (String) array.get(i);
            String item = itemStr.trim();
            String[] kvPairArr = item.split("=");
            String key = kvPairArr[0];
            Integer val = Integer.valueOf(kvPairArr[1]);
            Pair<String, Integer> kvPair = new Pair<>(key, val);
            rs.add(kvPair);
        }
        return rs;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

}

/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class DrdsToAutoSequenceUtil {
    final private static Long maxTimeBasedSeqOffInMs = 60 * 1000L;
    final private static Long maxGroupSeqOff = (long) SequenceAttribute.DEFAULT_INNER_STEP;

    protected static String generateNewSequenceSql(String seqName, Long startWithNum) {
        final String createSql = "create new sequence %s ";
        final String startWith = "start with %s ";
        if (startWithNum == null) {
            return String.format(createSql, SQLUtils.encloseWithUnquote(seqName));
        } else {
            return String.format(createSql, SQLUtils.encloseWithUnquote(seqName)) + String.format(startWith,
                startWithNum);
        }
    }

    protected static String generateGroupSequenceSql(String seqName, Long startWithNum, int unitCount, int unitIndex) {
        final String createSql = "create group sequence %s ";
        final String startWith = "start with %s ";
        String createSequenceSql;
        if (startWithNum == null) {
            createSequenceSql = String.format(createSql, SQLUtils.encloseWithUnquote(seqName));
        } else {
            createSequenceSql =
                String.format(createSql, SQLUtils.encloseWithUnquote(seqName)) + String.format(startWith,
                    startWithNum);
        }
        if (unitCount > 1) {
            createSequenceSql = createSequenceSql + String.format(" UNIT COUNT %d INDEX %d", unitCount, unitIndex);
        }
        return createSequenceSql;
    }

    protected static String generateRaiseNewSeqSql(String seqName, Long newStartWith) {
        final String sql = "alter sequence %s start with %s";
        return String.format(sql, SQLUtils.encloseWithUnquote(seqName), newStartWith);
    }

    public static Long getNextValueOfSeq(String schemaName, String seqName) {
        final String sql = "select %s.nextval";
        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(
            String.format(sql, SQLUtils.encloseWithUnquote(seqName)),
            schemaName,
            null
        );

        Long val = null;
        if (result.isEmpty() || result.get(0).isEmpty()) {
            throw new TddlNestableRuntimeException(
                String.format("acquire sequence %s failed in database[%s]", seqName, schemaName));
        }

        for (Map<String, Object> mp : result) {
            for (String key : mp.keySet()) {
                val = (Long) mp.get(key);
            }
        }
        return val;
    }

    public static String handleDrdsSequence(String schemaName, String name, String type, boolean generateNewSeq) {
        return handleDrdsSequence(schemaName, name, type, "", "", generateNewSeq, false);
    }

    public static String handleDrdsSequence(String schemaName,
                                            String name,
                                            String type,
                                            String unitCountStr,
                                            String unitIndexStr,
                                            boolean generateNewSeq,
                                            boolean useGroupSeqAsDefault) {
        Long newSeqVal = null;
        if (type.equalsIgnoreCase("time")) {
            Long val = getNextValueOfSeq(schemaName, name);
            newSeqVal = val + (maxTimeBasedSeqOffInMs << 22);
        } else if (type.equalsIgnoreCase("group")) {
            Long val = getNextValueOfSeq(schemaName, name);
            newSeqVal = val + maxGroupSeqOff;
        } else {
            Long val = getNextValueOfSeq(schemaName, name);
            newSeqVal = val + 1;
        }

        if (generateNewSeq) {
            if (useGroupSeqAsDefault && type.equalsIgnoreCase("group")) {
                int unitCount = 1;
                int unitIndex = 0;
                try {
                    unitCount = Integer.parseInt(unitCountStr);
                    unitIndex = Integer.parseInt(unitIndexStr);
                } catch (Exception e) {
                    // do nothing
                }
                return generateGroupSequenceSql(name, newSeqVal, unitCount, unitIndex);
            } else {
                return generateNewSequenceSql(name, newSeqVal);
            }
        } else {
            return generateRaiseNewSeqSql(name, newSeqVal);
        }
    }

    public static Map<String, String> convertAllDrdsSequences(List<String> allTableNames, String schemaName,
                                                              boolean useGroupSeqAsDefault) {
        final String querySql = "show sequences";
        Set<String> possibleAutoSeqName = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> result = new HashMap<>();
        for (String tbName : allTableNames) {
            possibleAutoSeqName.add(SequenceAttribute.AUTO_SEQ_PREFIX + tbName);
        }

        List<Map<String, Object>> allSeqRec = DdlHelper.getServerConfigManager().executeQuerySql(
            querySql,
            schemaName,
            null
        );

        for (Map<String, Object> seqRec : allSeqRec) {
            String name = (String) seqRec.get("NAME");
            String type = (String) seqRec.get("TYPE");
            String unitCount = (String) seqRec.get("UNIT_COUNT");
            String unitIndex = (String) seqRec.get("UNIT_INDEX");
            boolean generateNew = (!possibleAutoSeqName.contains(name));
            result.put(name,
                handleDrdsSequence(schemaName, name, type, unitCount, unitIndex, generateNew, useGroupSeqAsDefault));
        }

        return result;
    }

    public static Map<String, String> convertOnlyTableSequences(List<String> needCreateTableNames, String schemaName) {
        final String querySql = "show sequences";
        Set<String> needAlterSeqName = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> result = new HashMap<>();
        for (String tbName : needCreateTableNames) {
            needAlterSeqName.add(SequenceAttribute.AUTO_SEQ_PREFIX + tbName);
        }

        List<Map<String, Object>> allSeqRec = DdlHelper.getServerConfigManager().executeQuerySql(
            querySql,
            schemaName,
            null
        );

        for (Map<String, Object> seqRec : allSeqRec) {
            String name = (String) seqRec.get("NAME");
            String type = (String) seqRec.get("TYPE");
            if (needAlterSeqName.contains(name)) {
                result.put(name, handleDrdsSequence(schemaName, name, type, false));
            }
        }
        return result;
    }
}

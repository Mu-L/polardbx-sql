package com.alibaba.polardbx.druid.util;

import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.druid.sql.ast.TDDLStoragePartitionHint;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class StoragePartitionHintUtils {
    public static final String MYSQL_PARTITION_HINT_PREFIX_50100 = "!50100";
    public static final String MYSQL_PARTITION_HINT_PREFIX_50500 = "!50500";
    public static final String STORAGE_PARTITION_HINT_PREFIX = "!STORAGE";

    public static List<SQLCommentHint> convertStoragePartHitPrefixIntoMySqlPartPrefixIfNeed(
        List<SQLCommentHint> createTblOptionHints) {
        List<Integer> convertedSqlHintIndexList = new ArrayList<>();
        List<SQLCommentHint> newCreateTblOptionHints = convertOldPartHitPrefixIntoNewPartPrefixInner(
            createTblOptionHints,
            STORAGE_PARTITION_HINT_PREFIX,
            MYSQL_PARTITION_HINT_PREFIX_50100,
            convertedSqlHintIndexList);
        return newCreateTblOptionHints;
    }

    public static List<SQLCommentHint> convertMySqlPartHitPrefixIntoStoragePartPrefixIfNeed(
        List<SQLCommentHint> createTblOptionHints) {
        List<Integer> convertedSqlHintIndexList = new ArrayList<>();
        List<SQLCommentHint> newCreateTblOptionHints = convertOldPartHitPrefixIntoNewPartPrefixInner(
            createTblOptionHints,
            MYSQL_PARTITION_HINT_PREFIX_50500,
            STORAGE_PARTITION_HINT_PREFIX,
            convertedSqlHintIndexList);
        if (convertedSqlHintIndexList.isEmpty()) {
            newCreateTblOptionHints = convertOldPartHitPrefixIntoNewPartPrefixInner(
                createTblOptionHints,
                MYSQL_PARTITION_HINT_PREFIX_50100,
                STORAGE_PARTITION_HINT_PREFIX,
                convertedSqlHintIndexList);
        }
        return newCreateTblOptionHints;
    }

    protected static List<SQLCommentHint> convertOldPartHitPrefixIntoNewPartPrefixInner(
        List<SQLCommentHint> createTblOptionHints,
        String oldPartHintPrfix,
        String newPartHintPrfix,
        List<Integer> convertedSqlHintIndexList) {
        List<SQLCommentHint> newCreateTblOptionHints = new ArrayList<>();
        for (int i = 0; i < createTblOptionHints.size(); i++) {
            SQLCommentHint hint = createTblOptionHints.get(i);
            if (hint instanceof TDDLHint) {
                newCreateTblOptionHints.add(hint);
                continue;
            }
            SQLCommentHint newHint = null;
            String text = hint.getText();
            if (StringUtils.isEmpty(text)) {
                newCreateTblOptionHints.add(hint);
                continue;
            }
            if (!text.toUpperCase().contains("PARTITION BY")) {
                newCreateTblOptionHints.add(hint);
                continue;
            }
            int hintLength = text.length();
            int oldHintPrefixLength = oldPartHintPrfix.length();
            boolean findNewHint = false;
            String hintContentAfterPreFixStr = "";
            if (hintLength >= oldHintPrefixLength) {
                String prefixStr = text.substring(0, oldHintPrefixLength);

                if (prefixStr.equalsIgnoreCase(oldPartHintPrfix)) {
                    findNewHint = true;
                    hintContentAfterPreFixStr = text.substring(oldHintPrefixLength);

                }
            }
            if (findNewHint) {
                if (convertedSqlHintIndexList != null) {
                    convertedSqlHintIndexList.add(i);
                }
                String newHintText = newPartHintPrfix + hintContentAfterPreFixStr;
                if (newPartHintPrfix.equalsIgnoreCase(StoragePartitionHintUtils.STORAGE_PARTITION_HINT_PREFIX)) {
                    newHint = new TDDLStoragePartitionHint(newPartHintPrfix, hintContentAfterPreFixStr);
                } else {
                    newHint = new SQLCommentHint(newHintText);
                }
            } else {
                newHint = hint;
            }
            newCreateTblOptionHints.add(newHint);
        }
        return newCreateTblOptionHints;
    }
}

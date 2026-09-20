package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicateBuilder;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexTableInputRef;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LookupInfo {
    //private List<String> columnOrigins;

    private final List<LookupEquiJoinKey> allJoinKeys; // including null-safe equal (`<=>`)
    private final LookupPredicate predicates;
    private final Index lookupIndex;
    private final String joinName;
    private final boolean gsiLookup;
    private final boolean primaryHasGsiLocalIndex;

    public static LookupInfo EMPTY = new LookupInfo(null, null, null, "EMPTY", false, false);

    public LookupInfo(List<LookupEquiJoinKey> allJoinKeys, LookupPredicate predicates, Index lookupIndex,
                      String joinName, boolean gsiLookup, boolean primaryHasGsiLocalIndex) {
        this.allJoinKeys = allJoinKeys;
        this.predicates = predicates;
        this.lookupIndex = lookupIndex;
        this.joinName = joinName;
        this.gsiLookup = gsiLookup;
        this.primaryHasGsiLocalIndex = primaryHasGsiLocalIndex;
    }

    public static LookupInfo buildLookupInfo(LogicalView lv, Join join) {
        Preconditions.checkArgument(join instanceof LookupJoin);
        List<String> columnOrigins = Lists.newArrayList();
        RelMetadataQuery mq = lv.getCluster().getMetadataQuery();
        for (int i = 0; i < lv.getRowType().getFieldCount(); i++) {
            RelColumnOrigin columnOrigin = mq.getColumnOrigin(lv, i);
            if (columnOrigin == null) {
                columnOrigins.add(lv.getRowType().getFieldNames().get(i));
            } else {
                columnOrigins.add(columnOrigin.getColumnName());
            }
        }

        List<LookupEquiJoinKey> allJoinKeys =
            EquiJoinUtils.buildLookupEquiJoinKeys(join, join.getOuter(), join.getInner(),
                (RexCall) join.getCondition(), join.getJoinType());
        LookupPredicate predicates = new LookupPredicateBuilder(join, columnOrigins).build(allJoinKeys);
        Index lookupIndex = ((LookupJoin) join).getLookupIndex();
        String joinName = join.getClass().getSimpleName();
        boolean gsiLookup = false;
        boolean primaryHasGsiLocalIndex = false;
        if (BKAJoin.class.getSimpleName().equals(joinName)) {
            // outer is gsi
            Set<RexTableInputRef.RelTableRef> outerRefs = mq.getTableReferences(join.getOuter());
            Set<RexTableInputRef.RelTableRef> innerRefs = mq.getTableReferences(join.getInner());
            if (outerRefs != null && outerRefs.size() == 1 && innerRefs != null && innerRefs.size() == 1) {
                TableMeta outerTm = CBOUtil.getTableMeta(outerRefs.stream().findFirst().get().getTable());
                TableMeta innerTm = CBOUtil.getTableMeta(innerRefs.stream().findFirst().get().getTable());
                // inner is source table
                if (outerTm != null && innerTm != null) {
                    if (innerTm.withGsi(TddlSqlToRelConverter.unwrapGsiName(outerTm.getTableName()))) {
                        if (innerTm.getGsiPublished().containsKey(outerTm.getTableName())) {
                            GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean =
                                innerTm.getGsiPublished().get(outerTm.getTableName());
                            List<String> cols = gsiIndexMetaBean.getIndexColumns().stream()
                                .map(GsiMetaManager.GsiIndexColumnMetaBean::getColumnName)
                                .map(String::toLowerCase)
                                .collect(Collectors.toList());

                            for (IndexMeta im : innerTm.getIndexes()) {
                                if (prefixMatch(cols, im)) {
                                    primaryHasGsiLocalIndex = true;
                                }
                            }
                        }
                        gsiLookup = true;
                    }
                }
            }

        }
        return new LookupInfo(allJoinKeys, predicates, lookupIndex, joinName, gsiLookup, primaryHasGsiLocalIndex);
    }

    /**
     * Check if the given columns match the index key columns (prefix match).
     *
     * @param cols the column names to check
     * @param im the index meta to check against
     * @return true if all given columns form a prefix of the index key columns, false otherwise
     */
    public static boolean prefixMatch(List<String> cols, IndexMeta im) {
        if (cols == null || cols.isEmpty()) {
            return true;
        }

        if (im == null) {
            return false;
        }

        // Ensure all column names in cols are lowercase
        List<String> lowerCaseCols = cols.stream()
            .map(String::toLowerCase)
            .collect(Collectors.toList());

        // Get the key columns of the index
        List<ColumnMeta> indexKeyColumns = im.getKeyColumns();

        // If the number of index columns is less than the given columns, it cannot be a match
        if (indexKeyColumns.size() < lowerCaseCols.size()) {
            return false;
        }

        // Convert index key column names to a set for efficient lookup
        int matchCount = 0;
        for (ColumnMeta columnMeta : indexKeyColumns) {
            String indexColumnName = columnMeta.getName().toLowerCase();
            if (!lowerCaseCols.contains(indexColumnName)) {
                return false;
            }
            matchCount++;
            if (matchCount == cols.size()) {
                return true;
            }
        }

        if (matchCount == cols.size()) {
            return true;
        } else {
            return false;
        }
    }

    public Index getLookupIndex() {
        return lookupIndex;
    }

    public List<LookupEquiJoinKey> getAllJoinKeys() {
        return allJoinKeys;
    }

    public LookupPredicate getPredicates() {
        return predicates;
    }

    public boolean isMaterializedSemiJoin() {
        return MaterializedSemiJoin.class.getSimpleName().equals(joinName);
    }

    public boolean isGsiLookup() {
        return gsiLookup;
    }

    public boolean isPrimaryHasGsiLocalIndex() {
        return primaryHasGsiLocalIndex;
    }
}
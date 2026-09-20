package com.alibaba.polardbx.executor.gsi;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Utility class for building hash route conditions for inplace backfill operations.
 * This class provides methods to generate SQL conditions based on partition keys and hash value ranges.
 */
public class InplaceBackfillHashRouteUtils {

    public static SqlNode buildHashRouteCondition(
        List<List<String>> partitionKeys,
        List<com.alibaba.polardbx.common.utils.Pair<Long, Long>> targetTablePartitionBounds,
        List<com.alibaba.polardbx.common.utils.Pair<Long, Long>> sourceTablePartitionBounds,
        boolean withHashSpaceCheck) {

        if (partitionKeys == null || partitionKeys.isEmpty() ||
            targetTablePartitionBounds == null || targetTablePartitionBounds.isEmpty()) {
            return null;
        }

        int dim = partitionKeys.size();
        if (dim != targetTablePartitionBounds.size()) {
            throw new IllegalArgumentException("Dimension mismatch between partitionKeys and partitionBounds");
        }

        // Extract lower and upper bounds
        List<Long> lowerBound = new ArrayList<>(dim);
        List<Long> upperBound = new ArrayList<>(dim);
        for (com.alibaba.polardbx.common.utils.Pair<Long, Long> bound : targetTablePartitionBounds) {
            lowerBound.add(bound.getKey());
            upperBound.add(bound.getValue());
        }

        // === Case 1: HASH partitioning (single dimension, possibly multi-column) ===
        if (dim == 1) {
            long low = lowerBound.get(0);
            long high = upperBound.get(0);
            if (low >= high) {
                return null; // empty range
            }

            List<String> columns = partitionKeys.get(0);
            if (columns.isEmpty()) {
                return null;
            }

            // Build hash expression with optional hash space check for first dimension
            SqlNode hashExpr = buildHasherWithCheck(columns, 0, withHashSpaceCheck, sourceTablePartitionBounds);

            return new SqlBasicCall(SqlStdOperatorTable.BETWEEN,
                new SqlNode[] {hashExpr, buildLongLiteral(low), buildLongLiteral(high - 1)},
                SqlParserPos.ZERO);
        }

        // === Case 2: KEY partitioning (multi-dim, each dim exactly one column) ===
        for (int i = 0; i < dim; i++) {
            if (partitionKeys.get(i).size() != 1) {
                throw new IllegalArgumentException("In KEY partitioning, each dimension must have exactly one column");
            }
        }

        // Build hash expressions for each dimension
        // For first dimension, create both versions: with and without hash space check
        // We'll use the version with check only once in the first condition
        List<SqlNode> hashExprs = new ArrayList<>(dim);
        SqlNode firstDimHashExprWithCheck = null;
        for (int i = 0; i < dim; i++) {
            String col = partitionKeys.get(i).get(0);
            List<String> colList = Collections.singletonList(col);
            if (i == 0 && withHashSpaceCheck &&
                sourceTablePartitionBounds != null && !sourceTablePartitionBounds.isEmpty()) {
                // Create version with check for first dimension
                firstDimHashExprWithCheck = buildHasherWithCheck(colList, 0, true, sourceTablePartitionBounds);
                // Default version without check
                hashExprs.add(buildHasherWithCheck(colList, 0, false, sourceTablePartitionBounds));
            } else {
                hashExprs.add(buildHasherWithCheck(colList, i, false, sourceTablePartitionBounds));
            }
        }

        // Find first differing dimension k
        int k = 0;
        while (k < dim && Objects.equals(lowerBound.get(k), upperBound.get(k))) {
            k++;
        }
        if (k == dim) {
            return null; // L == U
        }

        // Check if any prefix dimension has MIN_VALUE or MAX_VALUE.
        // Since hashvalue will never equal MIN_VALUE or MAX_VALUE,
        // prefix condition (h(i) = MIN or h(i) = MAX) is always false.
        for (int i = 0; i < k; i++) {
            long val = lowerBound.get(i); // equals upperBound.get(i)
            if (val == Long.MIN_VALUE || val == Long.MAX_VALUE) {
                return null;
            }
        }
// ==========: Collapse if all suffix dimensions are fully free ==========
        boolean suffixFullyFree = (k < dim - 1);
        if (suffixFullyFree) {
            for (int i = k + 1; i < dim; i++) {
                if (lowerBound.get(i) != Long.MIN_VALUE || upperBound.get(i) != Long.MAX_VALUE) {
                    suffixFullyFree = false;
                    break;
                }
            }
        }

        if (suffixFullyFree) {
            long Lk = lowerBound.get(k);
            long Uk = upperBound.get(k);
            if (Lk > Uk) {
                return null;
            }
            List<SqlNode> preds = new ArrayList<>();
            // Use version with check for first dimension if available (only once)
            for (int i = 0; i < k; i++) {
                SqlNode expr = (i == 0 && firstDimHashExprWithCheck != null) ?
                    firstDimHashExprWithCheck : hashExprs.get(i);
                preds.add(equals(expr, lowerBound.get(i)));
            }
            SqlNode kExpr = (k == 0 && firstDimHashExprWithCheck != null) ?
                firstDimHashExprWithCheck : hashExprs.get(k);
            preds.add(new SqlBasicCall(SqlStdOperatorTable.BETWEEN,
                new SqlNode[] {kExpr, buildLongLiteral(Lk), buildLongLiteral(Uk)},
                SqlParserPos.ZERO));
            return combineConditionsWithAnd(preds);
        }

// Case 1: only last dimension differs → simple optimization
        if (k == dim - 1) {
            long Lk = lowerBound.get(k);
            long Uk = upperBound.get(k);
            if (Lk >= Uk) {
                return null;
            }
            List<SqlNode> preds = new ArrayList<>();
            // Use version with check for first dimension if available (only once)
            for (int i = 0; i < k; i++) {
                SqlNode expr = (i == 0 && firstDimHashExprWithCheck != null) ?
                    firstDimHashExprWithCheck : hashExprs.get(i);
                preds.add(equals(expr, lowerBound.get(i)));
            }
            SqlNode kExpr = (k == 0 && firstDimHashExprWithCheck != null) ?
                firstDimHashExprWithCheck : hashExprs.get(k);
            preds.add(new SqlBasicCall(SqlStdOperatorTable.BETWEEN,
                new SqlNode[] {kExpr, buildLongLiteral(Lk), buildLongLiteral(Uk - 1)},
                SqlParserPos.ZERO));
            return combineConditionsWithAnd(preds);
        }

// ========== Case 2: k < dim - 1 ==========
        long Lk = lowerBound.get(k);
        long Uk = upperBound.get(k);
        if (Lk >= Uk) {
            return null;
        }

        List<SqlNode> conditions = new ArrayList<>();
        // Track if we've used the version with check for first dimension
        boolean firstDimCheckUsed = false;

        // --- Left fragment: prefix = L[0:k], h_k = Lk, suffix >= L[k+1:] ---
        List<SqlNode> leftPreds = new ArrayList<>();
        // Track if we actually use the version with check in left fragment
        boolean leftFragmentUsesCheck = false;
        for (int i = 0; i < k; i++) {
            // Use version with check for first dimension only once (in first condition)
            SqlNode expr = (i == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) ?
                firstDimHashExprWithCheck : hashExprs.get(i);
            if (i == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) {
                firstDimCheckUsed = true;
                leftFragmentUsesCheck = true;
            }
            leftPreds.add(equals(expr, lowerBound.get(i)));
        }
        // Use version with check for first dimension if k == 0 and not used yet
        SqlNode kExpr = (k == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) ?
            firstDimHashExprWithCheck : hashExprs.get(k);
        if (k == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) {
            firstDimCheckUsed = true;
            leftFragmentUsesCheck = true;
        }
        leftPreds.add(equals(kExpr, Lk));
        // Only include h(k)=L[k] in suffixGe when suffix has wrap-around range
        boolean hasWrapAround = hasWrapAroundRange(lowerBound, upperBound, k + 1, dim);
        SqlNode suffixGe;
        if (hasWrapAround) {
            suffixGe = buildSuffixGe(hashExprs, lowerBound, k + 1, hashExprs.get(k), Lk);
        } else {
            suffixGe = buildSuffixGe(hashExprs, lowerBound, k + 1);
        }

        // --- Check if right fragment is meaningful ---
        boolean suffixHasEmptyRange = false;
        for (int i = k + 1; i < dim; i++) {
            if (isEmptyRange(lowerBound.get(i), upperBound.get(i))) {
                suffixHasEmptyRange = true;
                break;
            }
        }

        SqlNode suffixLt = null;
        boolean rightIsFree = true;

        if (!suffixHasEmptyRange) {
            // Only include h(k)=U[k] in suffixLt when suffix has wrap-around range
            // This ensures consistent format with expected outputs
            if (hasWrapAround) {
                suffixLt = buildSuffixLt(hashExprs, upperBound, k + 1, hashExprs.get(k), Uk);
            } else {
                suffixLt = buildSuffixLt(hashExprs, upperBound, k + 1);
            }
            rightIsFree = (suffixLt == null);
        }

        // --- Middle fragment: h_k from Lk+1 to (rightIsFree ? Uk : Uk-1) ---
        long midStart = Lk + 1;
        long midEnd;
        if (suffixHasEmptyRange) {
            midEnd = Uk - 1;
        } else {
            midEnd = rightIsFree ? Uk : (Uk - 1);
        }

        // Optimization: If suffixGe is null (always true) and midStart == Lk + 1,
        // we can merge Left and Middle fragments into h(k) BETWEEN Lk AND midEnd
        boolean canMergeLeftAndMiddle = (suffixGe == null && midStart <= midEnd && midStart == Lk + 1);

        if (canMergeLeftAndMiddle) {
            // Merge Left and Middle: prefix + h(k) BETWEEN Lk AND midEnd
            // If we marked firstDimCheckUsed in leftPreds but didn't actually use it (because we're merging),
            // we should reset it so we can use the version with check in the merged condition
            if (leftFragmentUsesCheck && firstDimHashExprWithCheck != null) {
                // We marked it but didn't actually use it (because we're merging), so reset it
                firstDimCheckUsed = false;
            }
            List<SqlNode> mergedPreds = new ArrayList<>();
            for (int i = 0; i < k; i++) {
                // Use version with check for first dimension only once
                SqlNode expr = (i == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) ?
                    firstDimHashExprWithCheck : hashExprs.get(i);
                if (i == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) {
                    firstDimCheckUsed = true;
                }
                mergedPreds.add(equals(expr, lowerBound.get(i)));
            }
            SqlNode kExprForMerge = (k == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) ?
                firstDimHashExprWithCheck : hashExprs.get(k);
            if (k == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) {
                firstDimCheckUsed = true;
            }
            mergedPreds.add(new SqlBasicCall(SqlStdOperatorTable.BETWEEN,
                new SqlNode[] {kExprForMerge, buildLongLiteral(Lk), buildLongLiteral(midEnd)},
                SqlParserPos.ZERO));
            conditions.add(combineConditionsWithAnd(mergedPreds));
        } else {
            // Generate Left and Middle fragments separately
            // Skip if suffixGe is always false
            if (isAlwaysFalse(suffixGe)) {
                // Left fragment is skipped, so reset firstDimCheckUsed if we marked it
                if (leftFragmentUsesCheck) {
                    firstDimCheckUsed = false;
                }
                // do nothing — skip this disjunct
            } else {
                if (suffixGe != null && !isAlwaysTrue(suffixGe)) {
                    leftPreds.add(suffixGe);
                }
                // If all preds are true-equivalent, we might get empty list, but combineConditionsWithAnd handles it
                conditions.add(combineConditionsWithAnd(leftPreds));
            }

            if (midStart <= midEnd) {
                List<SqlNode> midPreds = new ArrayList<>();
                for (int i = 0; i < k; i++) {
                    // Use version with check for first dimension if not used yet
                    SqlNode expr = (i == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) ?
                        firstDimHashExprWithCheck : hashExprs.get(i);
                    if (i == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) {
                        firstDimCheckUsed = true;
                    }
                    midPreds.add(equals(expr, lowerBound.get(i)));
                }
                // Use version with check for first dimension if k == 0 and not used yet
                SqlNode kExprForMid = (k == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) ?
                    firstDimHashExprWithCheck : hashExprs.get(k);
                if (k == 0 && !firstDimCheckUsed && firstDimHashExprWithCheck != null) {
                    firstDimCheckUsed = true;
                }
                midPreds.add(new SqlBasicCall(SqlStdOperatorTable.BETWEEN,
                    new SqlNode[] {kExprForMid, buildLongLiteral(midStart), buildLongLiteral(midEnd)},
                    SqlParserPos.ZERO));
                conditions.add(combineConditionsWithAnd(midPreds));
            }
        }

        // --- Right fragment: only if it has non-trivial suffix constraint ---
        if (!rightIsFree) {
            // suffixLt != null
            if (isAlwaysFalse(suffixLt)) {
                // skip
            } else {
                List<SqlNode> rightPreds = new ArrayList<>();
                for (int i = 0; i < k; i++) {
                    // Use version without check for first dimension (already used in left/middle fragment)
                    rightPreds.add(equals(hashExprs.get(i), upperBound.get(i)));
                }
                rightPreds.add(equals(hashExprs.get(k), Uk));
                if (!isAlwaysTrue(suffixLt)) {
                    rightPreds.add(suffixLt);
                }
                conditions.add(combineConditionsWithAnd(rightPreds));
            }
        }

        if (conditions.isEmpty()) {
            return null;
        }
        if (conditions.size() == 1) {
            return conditions.get(0);
        }
        return combineConditionsWithOr(conditions);
    }

    private static SqlNode buildSuffixGe(List<SqlNode> hashExprs, List<Long> lowerBound, int start) {
        return buildSuffixGe(hashExprs, lowerBound, start, null, null);
    }

    /**
     * Build suffix >= condition for lexicographic comparison.
     *
     * @param hashExprs Hash expressions for each dimension
     * @param lowerBound Lower bounds for each dimension
     * @param start Starting dimension index
     * @param kExpr Optional: hash expression for dimension k (to include h(k)=L[k] in later branches)
     * @param kBound Optional: lower bound for dimension k
     */
    private static SqlNode buildSuffixGe(List<SqlNode> hashExprs, List<Long> lowerBound, int start,
                                         SqlNode kExpr, Long kBound) {
        int n = hashExprs.size();

        // After MIN fold, add:
        boolean allFromStartAreMax = true;
        for (int i = start; i < n; i++) {
            if (lowerBound.get(i) != Long.MAX_VALUE) {
                allFromStartAreMax = false;
                break;
            }
        }
        if (allFromStartAreMax) {
            return SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        }
        // ===折叠优化 ===
        // 如果从 start 开始，只有第一个维度非 MIN，其余都是 MIN，则可折叠为 >=
        boolean allAfterAreMin = true;
        for (int i = start + 1; i < n; i++) {
            if (lowerBound.get(i) != Long.MIN_VALUE) {
                allAfterAreMin = false;
                break;
            }
        }

        if (allAfterAreMin) {
            long firstBound = lowerBound.get(start);
            if (firstBound == Long.MIN_VALUE) {
                return null; // 恒真
            } else {
                // (h_start, ...) >= (firstBound, MIN, MIN, ...) ⇨ h_start >= firstBound
                return new SqlBasicCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    new SqlNode[] {hashExprs.get(start), buildLongLiteral(firstBound)},
                    SqlParserPos.ZERO
                );
            }
        }
        // === 折叠优化结束 ===
        List<SqlNode> disjuncts = new ArrayList<>();

        for (int i = start; i < n - 1; i++) {
            long bound = lowerBound.get(i);
            // Skip if bound is MAX_VALUE: h(i) > MAX_VALUE is always false
            if (bound == Long.MAX_VALUE) {
                continue;
            }
            SqlNode greaterThan;
            if (bound == Long.MIN_VALUE) {
                greaterThan = null;
            } else {
                greaterThan = new SqlBasicCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    new SqlNode[] {hashExprs.get(i), buildLongLiteral(bound)},
                    SqlParserPos.ZERO
                );
            }

            List<SqlNode> conj = new ArrayList<>();
            boolean prefixAlwaysFalse = false;

            // For branches after the first one (i > start), add h(k) = L[k] condition if provided
            if (i > start && kExpr != null && kBound != null) {
                conj.add(equals(kExpr, kBound));
            }

            for (int j = start; j < i; j++) {
                long eqBound = lowerBound.get(j);
                if (eqBound == Long.MIN_VALUE || eqBound == Long.MAX_VALUE) {
                    // MIN_VALUE: h(j) = MIN_VALUE is impossible (unless hash happens to be MIN_VALUE)
                    // MAX_VALUE: h(j) = MAX_VALUE is impossible (unless hash happens to be MAX_VALUE)
                    prefixAlwaysFalse = true;
                    break;
                } else {
                    conj.add(equals(hashExprs.get(j), eqBound));
                }
            }
            if (prefixAlwaysFalse) {
                continue;
            }
            if (greaterThan != null) {
                conj.add(greaterThan);
            }
            if (!conj.isEmpty()) {
                disjuncts.add(combineConditionsWithAnd(conj));
            } else {
                return null;
            }
        }

        // Last dimension: >=
        {
            long bound = lowerBound.get(n - 1);
            if (bound == Long.MIN_VALUE) {
                // h >= MIN is always true, but this case should have been handled by the fold above.
                // Still, if reached, it means prefix must hold, and no extra condition needed.
                // However, to avoid adding redundant prefix-only disjunct, we rely on fold.
                // In practice, this branch may not be needed if fold works.
                // For safety, skip explicit handling here.
            } else if (bound == Long.MAX_VALUE) {
                // Do nothing: (h_suffix >= ..., MAX) requires h_last == MAX,
                // which is a single point and can be ignored in partition pruning.
                // So skip adding this disjunct entirely.
            } else {
                SqlNode ge = new SqlBasicCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    new SqlNode[] {hashExprs.get(n - 1), buildLongLiteral(bound)},
                    SqlParserPos.ZERO
                );

                List<SqlNode> conj = new ArrayList<>();
                boolean prefixAlwaysFalse = false;

                // For the last dimension branch, add h(k) = L[k] condition if provided
                // This is needed when there are prefix dimensions in suffix (n - 1 > start)
                if (n - 1 > start && kExpr != null && kBound != null) {
                    conj.add(equals(kExpr, kBound));
                }

                for (int j = start; j < n - 1; j++) {
                    long eqBound = lowerBound.get(j);
                    if (eqBound == Long.MIN_VALUE || eqBound == Long.MAX_VALUE) {
                        // MIN_VALUE or MAX_VALUE: h(j) = eqBound is impossible (unless hash happens to be that value)
                        prefixAlwaysFalse = true;
                        break;
                    } else {
                        conj.add(equals(hashExprs.get(j), eqBound));
                    }
                }
                if (!prefixAlwaysFalse) {
                    conj.add(ge);
                    disjuncts.add(combineConditionsWithAnd(conj));
                }
            }
        }

        if (disjuncts.isEmpty()) {
            return SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        }
        return combineConditionsWithOr(disjuncts);
    }

    private static SqlNode buildSuffixLt(List<SqlNode> hashExprs, List<Long> upperBound, int start) {
        return buildSuffixLt(hashExprs, upperBound, start, null, null);
    }

    /**
     * Build suffix < condition for lexicographic comparison.
     *
     * @param hashExprs Hash expressions for each dimension
     * @param upperBound Upper bounds for each dimension
     * @param start Starting dimension index
     * @param kExpr Optional: hash expression for dimension k (to include h(k)=U[k] in later branches)
     * @param kBound Optional: upper bound for dimension k
     */
    private static SqlNode buildSuffixLt(List<SqlNode> hashExprs, List<Long> upperBound, int start,
                                         SqlNode kExpr, Long kBound) {
        int n = hashExprs.size();

        boolean allFromStartAreMin = true;
        for (int i = start; i < n; i++) {
            if (upperBound.get(i) != Long.MIN_VALUE) {
                allFromStartAreMin = false;
                break;
            }
        }
        if (allFromStartAreMin) {
            return SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        }
        // 如果第一个维度是 MAX → 恒真
        if (upperBound.get(start) == Long.MAX_VALUE) {
            return null;
        }

        // 找到最后一个非-MAX位置（假设 MAX 只出现在尾部）
        int lastNonMax = start;
        for (int i = start; i < n; i++) {
            if (upperBound.get(i) != Long.MAX_VALUE) {
                lastNonMax = i;
            } else {
                break;
            }
        }

        // 判断是否可以折叠为 <=：仅当 (U_start, MAX, MAX, ...)
        boolean canFoldToLe = false;
        if (lastNonMax == start && start + 1 < n) {
            // 确保从 start+1 到末尾全是 MAX
            canFoldToLe = true;
            for (int i = start + 1; i < n; i++) {
                if (upperBound.get(i) != Long.MAX_VALUE) {
                    canFoldToLe = false;
                    break;
                }
            }
        }

        if (canFoldToLe) {
            return new SqlBasicCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                new SqlNode[] {hashExprs.get(start), buildLongLiteral(upperBound.get(start))},
                SqlParserPos.ZERO
            );
        }

        // 否则，严格按 < 展开
        List<SqlNode> disjuncts = new ArrayList<>();
        for (int i = start; i <= lastNonMax; i++) {
            long bound = upperBound.get(i);
            if (bound == Long.MAX_VALUE) {
                continue; // shouldn't happen before lastNonMax
            }

            List<SqlNode> conj = new ArrayList<>();
            boolean prefixAlwaysFalse = false;

            // For branches after the first one (i > start), add h(k) = U[k] condition if provided
            if (i > start && kExpr != null && kBound != null) {
                conj.add(equals(kExpr, kBound));
            }

            for (int j = start; j < i; j++) {
                long eqBound = upperBound.get(j);
                if (eqBound == Long.MAX_VALUE) {
                    prefixAlwaysFalse = true;
                    break;
                }
                conj.add(equals(hashExprs.get(j), eqBound));
            }
            if (prefixAlwaysFalse) {
                continue;
            }

            conj.add(new SqlBasicCall(
                SqlStdOperatorTable.LESS_THAN,
                new SqlNode[] {hashExprs.get(i), buildLongLiteral(bound)},
                SqlParserPos.ZERO
            ));

            disjuncts.add(combineConditionsWithAnd(conj));
        }

        if (disjuncts.isEmpty()) {
            return SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        }
        return combineConditionsWithOr(disjuncts);
    }

    // Utilities

    /**
     * Build POLARDBX_HASHER expression with optional hash space check.
     * If withHashSpaceCheck is true and sourceTablePartitionBounds is provided,
     * add range validation parameters for the first dimension.
     *
     * @param columns Column names for the hash function
     * @param dimIndex Dimension index (0 for first dimension)
     * @param withHashSpaceCheck Whether to add hash space check
     * @param sourceTablePartitionBounds Source table partition bounds for range validation
     * @return SqlNode representing POLARDBX_HASHER function call
     */
    private static SqlNode buildHasherWithCheck(List<String> columns, int dimIndex,
                                                boolean withHashSpaceCheck,
                                                List<com.alibaba.polardbx.common.utils.Pair<Long, Long>> sourceTablePartitionBounds) {
        List<SqlNode> args = new ArrayList<>();

        // Add column identifiers
        for (String col : columns) {
            args.add(new SqlIdentifier(col, SqlParserPos.ZERO));
        }

        // Add range validation parameters for first dimension if needed
        if (withHashSpaceCheck && dimIndex == 0 &&
            sourceTablePartitionBounds != null && !sourceTablePartitionBounds.isEmpty()) {
            com.alibaba.polardbx.common.utils.Pair<Long, Long> sourceBound = sourceTablePartitionBounds.get(0);
            if (sourceBound != null) {
                args.add(buildLongLiteral(sourceBound.getKey()));
                args.add(buildLongLiteral(sourceBound.getValue()));
            }
        }

        return new SqlBasicCall(SqlStdOperatorTable.POLARDBX_HASHER,
            args.toArray(new SqlNode[0]), SqlParserPos.ZERO);
    }

    private static SqlNode equals(SqlNode expr, long value) {
        return new SqlBasicCall(SqlStdOperatorTable.EQUALS,
            new SqlNode[] {expr, buildLongLiteral(value)},
            SqlParserPos.ZERO);
    }

    private static SqlNode buildLongLiteral(long value) {
        return SqlLiteral.createExactNumeric(String.valueOf(value), SqlParserPos.ZERO);
    }

    private static SqlNode combineConditionsWithAnd(List<SqlNode> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return null;
        }
        if (conditions.size() == 1) {
            return conditions.get(0);
        }
        SqlNode result = conditions.get(0);
        for (int i = 1; i < conditions.size(); i++) {
            result = new SqlBasicCall(SqlStdOperatorTable.AND,
                new SqlNode[] {result, conditions.get(i)},
                SqlParserPos.ZERO);
        }
        return result;
    }

    private static SqlNode combineConditionsWithOr(List<SqlNode> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return null;
        }
        if (conditions.size() == 1) {
            return conditions.get(0);
        }
        SqlNode result = conditions.get(0);
        for (int i = 1; i < conditions.size(); i++) {
            result = new SqlBasicCall(SqlStdOperatorTable.OR,
                new SqlNode[] {result, conditions.get(i)},
                SqlParserPos.ZERO);
        }
        return result;
    }

    private static boolean isAlwaysFalse(SqlNode node) {
        return node instanceof SqlLiteral &&
            Boolean.FALSE.equals(((SqlLiteral) node).getValueAs(Boolean.class));
    }

    private static boolean isAlwaysTrue(SqlNode node) {
        return node instanceof SqlLiteral &&
            Boolean.TRUE.equals(((SqlLiteral) node).getValueAs(Boolean.class));
    }

    /*private static boolean isEmptyRange(long low, long high) {
        if (low >= high) {
            // Special case: (MAX, MAX) is not empty (represents equality to MAX)
            if (low == Long.MAX_VALUE && high == Long.MAX_VALUE) {
                return false;
            }
            return true;
        }
        return false;
    }
    private static boolean isEmptyRange1(long low, long high) {
        // 正常情况：low < high，不是空范围
        if (low < high) {
            return false;
        }
        // 相等情况：low == high，表示一个点，不是空范围
        if (low == high) {
            return false;
        }
        // 特殊情况：(MAX, MAX)表示等于MAX的点
        if (low == Long.MAX_VALUE && high == Long.MAX_VALUE) {
            return false;
        }
        // 在字典序多维哈希中，(large_positive, negative)是一种有效的跨边界范围
        // 例如：(接近MAX的值, 负值)表示从该值到MAX，然后从MIN到负值的范围
        // 这种情况在字典序排序中是有效的范围，不应被视为空范围
        if (low > 0 && high < 0) {
            return false; // 这是有效的跨边界范围
        }
        // 其他low > high的情况被认为是空范围
        return true;
    }*/

    private static boolean isEmptyRange(long low, long high) {
        // Case 1: Normal range low < high: not empty
        if (low < high) {
            return false;
        }

        // Case 2: Point range low == high
        // In lexicographic multi-dimension context, (n, n) is NOT empty because:
        // - Left fragment uses suffixGe: h(i) >= n
        // - Right fragment uses suffixLt: h(i) < n
        // Both conditions are valid and should be generated.
        // Only (MIN, MIN) is considered empty since h(i) < MIN is impossible.
        if (low == high) {
            return low == Long.MIN_VALUE;
        }

        // Case 3: Wrap-around range low > high
        // In lexicographic order with wrap-around semantics, (low, high) where low > high
        // represents a valid wrap-around range: [low, MAX_VALUE) ∪ [MIN_VALUE, high)
        // Since hashvalue won't equal MIN_VALUE or MAX_VALUE, this is NOT an empty range
        return false;
    }

    /**
     * Check if any dimension in the range [start, end) has a wrap-around range (L > U).
     */
    private static boolean hasWrapAroundRange(List<Long> lowerBound, List<Long> upperBound, int start, int end) {
        for (int i = start; i < end; i++) {
            if (lowerBound.get(i) > upperBound.get(i)) {
                return true;
            }
        }
        return false;
    }
}
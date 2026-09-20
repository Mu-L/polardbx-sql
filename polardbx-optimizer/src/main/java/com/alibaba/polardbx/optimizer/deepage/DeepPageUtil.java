package com.alibaba.polardbx.optimizer.deepage;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BigIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.ByteType;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.MediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.ShortType;
import com.alibaba.polardbx.optimizer.core.datatype.SmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.UMediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.USmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UTinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.datatype.YearType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ToDrdsRelVisitor;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.PreparedParamRef;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DeepPageUtil {

    private final static Logger logger = LoggerFactory.getLogger(DeepPageUtil.class);

    /**
     * 判断当前查询是否为翻页SQL，是则为其生成DeepPageCache
     */
    public static DeepPageCache tryCreateDeepPageCache(ExecutionPlan executionPlan, ExecutionContext ec) {
        try {
            if (ec.getDeepPageCacheMap() == null) {
                return null;
            }
            if (executionPlan.isExplain()) {
                return null;
            }
            //select
            if (!executionPlan.getAst().isA(SqlKind.QUERY)) {
                return null;
            }

            SqlParameterized sqlParameterized = getSqlParameterizedFromExecutionContext(executionPlan, ec);
            if (sqlParameterized == null) {
                return null;
            }

            List<Object> sqlParams = sqlParameterized.getParameters();

            PlanCache.CacheKey cacheKey = PlanCache.getCacheKey(ec.getSchemaName(), sqlParameterized, ec, false);
            DeepPageCache deepPageCache = ec.getDeepPageCacheMap().get(cacheKey);

            if (deepPageCache != null) {
                //limit offset必须连续
                if (deepPageCache.deepPageOffset == Long.parseLong(
                    sqlParams.get(deepPageCache.offsetParamIndex - 1).toString())) {
                    return deepPageCache;
                }
            }

            RelNode originPlan = getUnOptimizePlanFromExecutionPlan(executionPlan, ec);
            if (originPlan == null) {
                return null;
            }

            DeepPageFinder deepPageFinder = new DeepPageFinder();
            deepPageFinder.go(originPlan);

            //符合翻页sql
            if (!deepPageFinder.isDeepPage()) {
                return null;
            }
            DeepPageType deepPageType = deepPageFinder.getDeepPageType();
            if (deepPageType == null) {
                return null;
            }

            //orderby列均为select列,(如果没有select出来，最后怎么更新翻页缓存呢)
            List<Pair<String, String>> orderByColumns = deepPageFinder.getOrderByColumns();
            if (orderByColumns == null) {
                return null;
            }

            boolean orderByUniqueKey = isOrderByUniqueKey(ec.getSchemaManager(), orderByColumns, deepPageType);
            if (!orderByUniqueKey && deepPageType != DeepPageType.SIMPLE) {
                return null;
            }

            if (!isOrderByColTypeSupported(ec.getSchemaManager(), orderByColumns, orderByUniqueKey)) {
                return null;
            }

            List<Integer> orderByColIndexes =
                mapOrderByColToSelectCol(ec.getSchemaManager(), originPlan, orderByColumns, !orderByUniqueKey);
            if (orderByColIndexes == null) {
                return null;
            }

            DeepPageOptimizer deepPageOptimizer =
                new DeepPageOptimizer(ec.getSchemaManager(), deepPageFinder.isAsc(), orderByUniqueKey, sqlParams.size(),
                    deepPageType);
            RelNode deepPagePlan = originPlan.accept(deepPageOptimizer);
            if (deepPagePlan == originPlan) {
                return null;
            }

            //首次生成deepPageCache
            deepPageCache = new DeepPageCache(deepPagePlan, false, orderByColIndexes,
                deepPageFinder.getOffsetParamIndex(), deepPageFinder.getFetchParamIndex(), sqlParams);

            setDeepCache(ec, cacheKey, deepPageCache);

            return deepPageCache;
        } catch (Throwable e) {
            logger.error("tryCreateDeepPageCache error", e);
            return null;
        }

    }

    /**
     * 判断当前查询是否有对应的DeepPageCache，是则基于DeepPageCache进行查询计划改写
     */
    public static ExecutionPlan tryCreateDeepPageExecutionPlan(ExecutionPlan executionPlan, ExecutionContext ec) {
        try {
            if (ec.getDeepPageCacheMap() == null) {
                return null;
            }

            //select
            if (!executionPlan.getAst().isA(SqlKind.QUERY)) {
                return null;
            }

            SqlParameterized sqlParameterized = getSqlParameterizedFromExecutionContext(executionPlan, ec);
            if (sqlParameterized == null) {
                return null;
            }

            List<Object> sqlParams = sqlParameterized.getParameters();

            PlanCache.CacheKey cacheKey = PlanCache.getCacheKey(ec.getSchemaName(), sqlParameterized, ec, false);
            DeepPageCache deepPageCache = ec.getDeepPageCacheMap().get(cacheKey);
            if (deepPageCache == null) {
                return null;
            }

            RelNode deepPagePlan = deepPageCache.getDeepPagePlan();
            Pair<Long, List<Object>> deepPageOffsetAndParams = deepPageCache.getDeepPageOffsetAndParams();
            long deepPageOffset = deepPageOffsetAndParams.getKey();
            List<Object> deepPageParams = deepPageOffsetAndParams.getValue();
            if (deepPageParams == null) {
                return null;
            }
            //如果参数有null，则不能更改执行计划，与null比大小不符合规范
            for (Object deepPageParam : deepPageParams) {
                if (deepPageParam == null) {
                    return null;
                }
            }

            long sqlOffset = Long.parseLong(sqlParams.get(deepPageCache.getOffsetParamIndex() - 1).toString());

            //offset非连续，不能更改执行计划
            if (deepPageOffset != sqlOffset) {
                removeDeepPageCache(ec, cacheKey);
                return null;
            }

            //除了limit参数外，其他参数必须相同
            if (sqlParams.size() != deepPageCache.getSqlParams().size()) {
                removeDeepPageCache(ec, cacheKey);
                return null;
            }
            for (int i = 0; i < deepPageCache.getSqlParams().size(); i++) {
                if (i == deepPageCache.getOffsetParamIndex() - 1 || i == deepPageCache.getFetchParamIndex() - 1) {
                    continue;
                }
                if (!Objects.equals(sqlParams.get(i), deepPageCache.getSqlParams().get(i))) {
                    removeDeepPageCache(ec, cacheKey);
                    return null;
                }
            }

            //更新参数
            Map<Integer, ParameterContext> newSqlParamsMap = new HashMap<>(ec.getParamMap());
            //将offset参数更新为0
            newSqlParamsMap.put(deepPageCache.getOffsetParamIndex(),
                new ParameterContext(ParameterMethod.setInt, new Object[] {deepPageCache.getOffsetParamIndex(), 0}));
            //增加新的参数
            int deePageParamsIndex = deepPageCache.getSqlParams().size() + 1;
            for (Object param : deepPageParams) {
                newSqlParamsMap.put(deePageParamsIndex,
                    new ParameterContext(ParameterMethod.setObject1, new Object[] {deePageParamsIndex, param}));
                deePageParamsIndex++;
            }
            ec.getParams().setParams(newSqlParamsMap);
            ec.setPlanSource(PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER);

            //首次生成optimized deepPagePlan
            if (!deepPageCache.isOptimized) {
                PlannerContext plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());
                plannerContext.setParams(ec.getParams().clone());
                deepPagePlan = Planner.getInstance().optimize(deepPagePlan, plannerContext);
                deepPageCache.setOptimizedDeepPagePlan(deepPagePlan);
            }

            //更新执行计划
            PlannerContext.getPlannerContext(executionPlan.getPlan()).setPlanInfo(null);
            PlannerContext.getPlannerContext(executionPlan.getPlan()).setBaselineInfo(null);
            executionPlan = executionPlan.copy(deepPagePlan);

            return executionPlan;
        } catch (Throwable e) {
            logger.error("tryCreateDeepPageExecutionPlan error", e);
            return null;
        }
    }

    public static void setDeepCache(ExecutionContext ec, PlanCache.CacheKey cacheKey, DeepPageCache deepPageCache) {
        ec.getDeepPageCacheMap().put(cacheKey, deepPageCache);
    }

    public static void removeDeepPageCache(ExecutionContext ec, PlanCache.CacheKey cacheKey) {
        ec.getDeepPageCacheMap().remove(cacheKey);
    }

    private static SqlParameterized getSqlParameterizedFromExecutionContext(ExecutionPlan executionPlan,
                                                                            ExecutionContext ec) {
        if (ec.getSqlParameterized() != null && !ec.getSqlParameterized().isUnparameterized()) {
            return ec.getSqlParameterized();
        }

        if (ec.getPreparedStmtCache() != null && ec.getPreparedStmtCache().getSqlParameterized() != null
            && !ec.getPreparedStmtCache().getSqlParameterized().isUnparameterized()) {
            SqlParameterized sqlParameterized = ec.getPreparedStmtCache().getSqlParameterized();
            List<Object> parameters = sqlParameterized.getParameters()
                .stream()
                .map(p -> p instanceof PreparedParamRef ? ((PreparedParamRef) p).getValue() : p)
                .collect(Collectors.toList());
            return new SqlParameterized(sqlParameterized.getOriginSql(), sqlParameterized.getSql(), parameters,
                sqlParameterized.getStmt(), false);
        }

        String sql = ec.getOriginSql();

        //for explain
        if (executionPlan.isExplain()) {
            sql = getQueryAfterExplain(ec.getSql()).toString();
        }

        return SqlParameterizeUtils.parameterize(sql);
    }

    public static ByteString getQueryAfterExplain(ByteString explainQuery) {
        MySqlLexer lexer = new MySqlLexer(explainQuery);
        while (true) {
            lexer.nextToken();
            if (lexer.token() == Token.EXPLAIN) {
                lexer.nextToken(); // Move to next token after EXPLAIN
                if (lexer.identifierEquals("EXECUTE")) {
                    lexer.nextToken();
                }
                if (lexer.identifierEquals("COST")) {
                    lexer.nextToken();
                }
                return explainQuery.slice(lexer.getStartPos());
            }
        }
    }

    private static RelNode getUnOptimizePlanFromExecutionPlan(ExecutionPlan executionPlan, ExecutionContext ec) {
        RelNode drdsRelNode = null;
        SqlConverter converter = SqlConverter.getInstance(ec.getSchemaName(), ec);
        PlannerContext plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());
        RelNode originPlan = converter.toRel(converter.validate(executionPlan.getAst()), plannerContext);
        ToDrdsRelVisitor toDrdsRelVisitor = new ToDrdsRelVisitor(executionPlan.getAst(), plannerContext);
        drdsRelNode = originPlan.accept(toDrdsRelVisitor);
        return drdsRelNode;
    }

    public static boolean isOrderByColTypeSupported(SchemaManager schemaManager,
                                                    List<Pair<String, String>> orderByColumns,
                                                    boolean orderByUniqueKey) {
        if (orderByColumns == null || orderByColumns.isEmpty()) {
            return false;
        }
        for (Pair<String, String> orderByColumn : orderByColumns) {
            ColumnMeta columnMeta = schemaManager.getTable(orderByColumn.getKey()).getColumn(orderByColumn.getValue());
            if (columnMeta == null) {
                return false;
            }
            DataType dataType = columnMeta.getDataType();
            if (!isOrderByColTypeSupported(dataType)) {
                return false;
            }
        }
        if (!orderByUniqueKey) {
            TableMeta tableMeta = schemaManager.getTable(orderByColumns.get(0).getKey());
            for (ColumnMeta columnMeta : tableMeta.getPrimaryKey()) {
                DataType dataType = columnMeta.getDataType();
                if (!isOrderByColTypeSupported(dataType)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isOrderByColTypeSupported(DataType dataType) {
        return (dataType instanceof IntegerType
            || dataType instanceof ShortType
            || dataType instanceof BigIntegerType
            || dataType instanceof ULongType
            || dataType instanceof ByteType
            || dataType instanceof LongType
            || dataType instanceof DecimalType
            || dataType instanceof DateType
            || dataType instanceof DateTimeType
            || dataType instanceof TimeType
            || dataType instanceof TimestampType
            || dataType instanceof YearType
            || dataType instanceof CharType
            || dataType instanceof VarcharType);
    }

    public static boolean isOneTable(ExecutionPlan executionPlan) {
        Set<Pair<String, String>> tableSet = executionPlan.getTableSet();
        if (tableSet == null) {
            tableSet = PlanManagerUtil.getTableSetFromAst(executionPlan.getAst());
        }
        return tableSet.size() == 1;
    }

    public static boolean isOrderByUniqueKey(SchemaManager schemaManager, List<Pair<String, String>> orderByColumns,
                                             DeepPageType deepPageType) {

        if (deepPageType == DeepPageType.SIMPLE) {
            TableMeta tableMeta = schemaManager.getTable(orderByColumns.get(0).getKey());
            Set<String> orderByColSet = orderByColumns.stream().map(Pair::getValue).collect(Collectors.toSet());

            IndexMeta pkIndexMeta = tableMeta.getPrimaryIndex();
            if (pkIndexMeta != null) {
                boolean containPK = true;
                for (ColumnMeta columnMeta : pkIndexMeta.getKeyColumns()) {
                    if (!orderByColSet.contains(columnMeta.getName().toLowerCase())) {
                        containPK = false;
                        break;
                    }
                }
                if (containPK) {
                    return true;
                }
            }

            if (tableMeta.getGsiPublished() != null) {
                for (GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean : tableMeta.getGsiPublished().values()) {
                    if (gsiIndexMetaBean.nonUnique) {
                        continue;
                    }
                    boolean containUK = true;
                    for (GsiMetaManager.GsiIndexColumnMetaBean gsiIndexColumnMetaBean : gsiIndexMetaBean.indexColumns) {
                        if (!orderByColSet.contains(gsiIndexColumnMetaBean.columnName.toLowerCase())) {
                            containUK = false;
                            break;
                        }
                    }
                    if (containUK) {
                        return true;
                    }
                }
            }
        } else if (deepPageType == DeepPageType.JOIN) {
            if (orderByColumns.size() != 2) {
                return false;
            }
            if (orderByColumns.get(0).getKey().equalsIgnoreCase(orderByColumns.get(1).getKey())) {
                return false;
            }
            for (Pair<String, String> pair : orderByColumns) {
                IndexMeta pkIndexMeta = schemaManager.getTable(pair.getKey()).getPrimaryIndex();
                if (pkIndexMeta == null) {
                    return false;
                }
                if (pkIndexMeta.getKeyColumns().size() != 1) {
                    return false;
                }
                if (!pkIndexMeta.getKeyColumns().get(0).getName().equalsIgnoreCase(pair.getValue())) {
                    return false;
                }
            }
            return true;
        } else if (deepPageType == DeepPageType.AGG) {
            return true;
        }

        return false;
    }

    /**
     * 映射orderBy列到select列
     */
    public static List<Integer> mapOrderByColToSelectCol(SchemaManager schemaManager, RelNode node,
                                                         List<Pair<String, String>> orderByColumns, boolean appendPk) {
        List<Integer> colIndexes = new ArrayList<>();

        RelMetadataQuery mq = node.getCluster().getMetadataQuery();
        int outputFiledCount = node.getRowType().getFieldCount();
        Map<Pair<String, String>, Integer> outputTableColumns = new HashMap<>();
        for (int i = 0; i < outputFiledCount; i++) {
            RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(node, i);
            if (relColumnOrigin != null && !relColumnOrigin.isDerived()) {
                Pair<String, String> tableColumn = Pair.of(
                    relColumnOrigin.getOriginTable().getQualifiedName()
                        .get(relColumnOrigin.getOriginTable().getQualifiedName().size() - 1).toLowerCase(),
                    relColumnOrigin.getColumnName().toLowerCase()
                );
                outputTableColumns.put(tableColumn, i);
            }
        }

        for (Pair<String, String> orderByCol : orderByColumns) {
            Integer colIndex = outputTableColumns.get(orderByCol);
            if (colIndex == null) {
                return null;
            }
            colIndexes.add(colIndex);
        }

        String tableName = orderByColumns.get(0).getKey();
        TableMeta tableMeta = schemaManager.getTable(tableName);

        if (appendPk) {
            for (ColumnMeta pkCol : tableMeta.getPrimaryKey()) {
                Integer colIndex = outputTableColumns.get(Pair.of(tableName, pkCol.getName().toLowerCase()));
                if (colIndex == null) {
                    return null;
                }
                colIndexes.add(colIndex);
            }
        }

        return colIndexes;
    }

}

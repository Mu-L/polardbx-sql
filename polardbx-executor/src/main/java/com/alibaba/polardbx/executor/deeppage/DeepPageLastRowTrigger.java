package com.alibaba.polardbx.executor.deeppage;

import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.ResultSetRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.deepage.DeepPageCache;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class DeepPageLastRowTrigger implements LastRowTrigger {

    private static final Logger logger = LoggerFactory.getLogger(DeepPageLastRowTrigger.class);

    private DeepPageCache deepPageCache;

    private ExecutionContext ec;

    public DeepPageLastRowTrigger(DeepPageCache deepPageCache, ExecutionContext ec) {
        this.deepPageCache = deepPageCache;
        this.ec = ec;
    }

    @Override
    public void onLastRow(Row row, CursorMeta cursorMeta, long rows) {
        try {
            if (row == null) {
                return;
            }
            if (row instanceof ResultSetRow) {
                throw new NotSupportException("deep page optimizer do not support ResultRow");
            }

            long newCacheOffset = deepPageCache.getDeepPageOffsetAndParams().getKey()
                + Long.parseLong(
                ec.getParams().getCurrentParameter().get(deepPageCache.getOffsetParamIndex()).getValue().toString())
                + rows;

            List<Integer> orderByColIndexes = deepPageCache.getOrderByColIndexes();
            List<Object> orderByValues = new ArrayList<>(orderByColIndexes.size());

            for (Integer colIndex : orderByColIndexes) {
                ParameterContext parameterContext = Transformer.buildColumnParam(row, colIndex);
                orderByValues.add(parameterContext.getValue());
            }

            deepPageCache.updateCacheOffsetAndParams(newCacheOffset, orderByValues);
        } catch (Throwable e) {
            logger.error("update deep page cache offset and params error", e);
        }

    }
}

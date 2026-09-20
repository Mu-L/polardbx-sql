package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

/**
 * CONVERT_TZ(dt,from_tz,to_tz)
 * CONVERT_TZ() converts a datetime value dt from the time zone given by from_tz to the time
 * zone given by to_tz and returns the resulting value.
 */
public class ConvertTz extends AbstractScalarFunction {
    public ConvertTz() {
    }

    public ConvertTz(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }

        }

        String fromTimezoneId = DataTypes.StringType.convertFrom(args[1]);
        String toTimezoneId = DataTypes.StringType.convertFrom(args[2]);

        ZoneId fromZoneId = TimeZoneUtils.zoneIdOf(fromTimezoneId);
        ZoneId toZoneId = TimeZoneUtils.zoneIdOf(toTimezoneId);

        if (fromZoneId == null || toZoneId == null) {
            return null;
        }
        TimeZone toTimeZone = TimeZone.getTimeZone(toTimezoneId);

        Object timeObj = args[0];
        MysqlDateTime t =
            DataTypeUtil.toMySQLDatetimeByFlags(timeObj, Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);

        // interpret the datetime to From_Timezone.
        ZonedDateTime zonedDateTime = MySQLTimeTypeUtil.toZonedDatetime(t, fromZoneId);
        if (zonedDateTime == null) {
            return null;
        }

        // convert to To_Timezone
        zonedDateTime = zonedDateTime.withZoneSameInstant(toZoneId);
        MysqlDateTime ret = MySQLTimeTypeUtil.fromZonedDatetime(zonedDateTime);

        return DataTypeUtil.fromMySQLDatetime(getReturnType(), ret, toTimeZone);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CONVERT_TZ"};
    }

    /**
     * AbstractScalarFunction 仅按 resultType 推导返回类型，而调用方也可能只设置 resultField，
     * 因此优先取 resultField，避免返回类型为 null 时退化成字符串。
     */
    @Override
    public DataType getReturnType() {
        if (resultField != null) {
            return resultField.getDataType();
        }
        return super.getReturnType();
    }
}

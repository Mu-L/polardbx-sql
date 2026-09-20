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

package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;

import java.util.Iterator;
import java.util.List;

/**
 * This class is not fully implemented yet.
 * This can be used while converting from jdbc type to Polardb-X type, and cannot be used directly in optimizer
 */
public class SetSqlType extends BasicSqlType {
    private final List<String> setValues;

    public SetSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName, List<String> setValues) {
        super(typeSystem, typeName);
        this.setValues = setValues;
    }

    public SetSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName, int precision, List<String> setValues) {
        super(typeSystem, typeName, precision);
        this.setValues = setValues;
    }

    public List<String> getSetValues() {
        return setValues;
    }

    @Override
    public void generateTypeString(StringBuilder sb, boolean withDetail) {
        // Called to make the digest, which equals() compares;
        // so equivalent data types must produce identical type strings.

        sb.append("SET");

        if (setValues != null) {
            sb.append('(');
            final Iterator<String> iterator = setValues.iterator();
            boolean isFirst = true;
            while (iterator.hasNext()) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    sb.append(", ");
                }
                final String next = iterator.next();
                sb.append(next);
            }
            sb.append(')');
        }
        if (!withDetail) {
            return;
        }
        if (getCharset() != null) {
            sb.append(" CHARACTER SET \"");
            sb.append(getCharset().name());
            sb.append("\"");
        }
        if (getCollation() != null) {
            sb.append(" COLLATE \"");
            sb.append(getCollation().getCollationName());
            sb.append("\"");
        }
    }

}

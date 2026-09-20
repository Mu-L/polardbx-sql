package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.optimizer.config.table.SkipSerializeInDdlDumpInfo;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

import javax.validation.valueextraction.Unwrapping;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableMetaSerializer {
    public static String serializeObject(TableMeta obj) {
        TableMetaSerializer tableMetaSerializer = new TableMetaSerializer();
        return tableMetaSerializer.serialize(obj);
    }

    private Set<Object> objectSet = new HashSet<>();

    public TableMetaSerializer() {
    }

    public String serialize(Object obj) {
        if (obj instanceof List && ((List<?>) obj).size() == 0) {
            return "null";
        }
        if (obj == null) {
            return "null";
        }
        Class<?> clazz = obj.getClass();

        if (isSimpleType(clazz)) {
            return obj.toString();
        }
        if (clazz.isEnum()) {
            return ((Enum<?>) obj).name();
        }
        if (obj instanceof com.alibaba.polardbx.optimizer.config.table.Field) {
            return obj.toString();
        }
        if (objectSet.contains(obj)) {
            return "REF";
        }
        objectSet.add(obj);
        if (clazz.isArray()) {
            return serializeArray(obj);
        } else if (Map.class.isAssignableFrom(clazz)) {
            return serializeMap((Map<?, ?>) obj);
        } else if (Collection.class.isAssignableFrom(clazz)) {
            return serializeCollection((Collection<?>) obj);
        } else {
            return serializeObject(obj);
        }
    }

    /**
     * 判断是否是简单类型（基本类型或其包装类、String、Date）
     */
    private static boolean isSimpleType(Class<?> clazz) {
        return clazz.isPrimitive() ||
            clazz == String.class ||
            clazz == Integer.class ||
            clazz == Long.class ||
            clazz == Double.class ||
            clazz == Float.class ||
            clazz == Boolean.class ||
            clazz == Character.class ||
            clazz == Byte.class ||
            clazz == Short.class ||
            clazz == Date.class;
    }

    /**
     * 序列化数组
     */
    private String serializeArray(Object array) {
        int length = Array.getLength(array);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(serialize(Array.get(array, i)));
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * 序列化 Map
     */
    private String serializeMap(Map<?, ?> map) {
        StringBuilder sb = new StringBuilder("{");
        int i = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (i++ > 0) {
                sb.append(", ");
            }
            sb.append(serialize(entry.getKey()))
                .append(": ")
                .append(serialize(entry.getValue()));
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * 序列化集合（List、Set 等）
     */
    private String serializeCollection(Collection<?> collection) {
        StringBuilder sb = new StringBuilder("[");
        int i = 0;
        for (Object element : collection) {
            if (i++ > 0) {
                sb.append(", ");
            }
            sb.append(serialize(element));
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * 序列化复合类型（自定义类）
     */
    private String serializeObject(Object obj) {
        Class<?> clazz = obj.getClass();
        StringBuilder sb = new StringBuilder(clazz.getSimpleName()).append("{");
        Field[] fields = clazz.getDeclaredFields();
        int i = 0;
        SkipSerializeInDdlDumpInfo skipSerializeInDdlDumpInfo = clazz.getAnnotation(SkipSerializeInDdlDumpInfo.class);
        Set<String> skipSet = new HashSet<>();
        if (skipSerializeInDdlDumpInfo != null) {
            skipSet = new HashSet<>(Arrays.asList(skipSerializeInDdlDumpInfo.value()));
        }
        for (Field field : fields) {
            field.setAccessible(true);
            if (skipSet.contains(field.getName()) || Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(
                field.getModifiers())) {
                continue;
            }
            try {
                if (i++ > 0) {
                    sb.append(", ");
                }
                Object value = field.get(obj);
                sb.append(field.getName()).append("=").append(serialize(value));
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Error accessing field: " + field.getName(), e);
            }
        }
        sb.append("}");
        return sb.toString();
    }
}

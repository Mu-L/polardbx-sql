package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.google.common.base.Preconditions;
import org.apache.orc.customized.ORCFieldMemoryCounter;
import org.openjdk.jol.util.VMSupport;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AnnotationBasedMemoryCounter implements FastMemoryCounter {

    private final int maxDepth;
    private final boolean useFieldMemoryCounter;
    private final boolean generateFieldSizeMap;
    private final boolean generateTreeStruct;
    private final ConditionalMemoryType conditionalMemoryType;

    private static ThreadLocal<Set<String>> threadLocalVisitedAddressSet =
        ThreadLocal.withInitial(() -> new HashSet<>());

    private Map<String, Integer> fieldSizeMap = new HashMap<>();
    private long totalSize;

    private TreeNode rootTreeNode;
    private Map<TreeNode, List<TreeNode>> memoryUsageTree = new HashMap<>();

    private static class TreeNode {
        final String fieldIdentifier;
        final long memorySize;

        boolean fieldMemoryVisible;
        long accumulatedMemorySize = 0L;

        TreeNode(String fieldIdentifier, long memorySize) {
            this.fieldIdentifier = fieldIdentifier;
            this.memorySize = memorySize;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            TreeNode treeNode = (TreeNode) object;
            return Objects.equals(fieldIdentifier, treeNode.fieldIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fieldIdentifier);
        }

        static String objectIdentifier(Object o) {
            return o.getClass().getSimpleName() + "@" + System.identityHashCode(o);
        }

        static String fieldIdentifier(Field f) {
            return f.getName();
        }
    }

    public AnnotationBasedMemoryCounter() {
        this(DEFAULT_MAX_DEPTH, true, false, false, null);
    }

    public AnnotationBasedMemoryCounter(int maxDepth, boolean useFieldMemoryCounter, boolean generateFieldSizeMap,
                                        boolean generateTreeStruct,
                                        ConditionalMemoryType conditionalMemoryType) {
        this.maxDepth = maxDepth;
        this.useFieldMemoryCounter = useFieldMemoryCounter;
        this.generateFieldSizeMap = generateFieldSizeMap;
        this.generateTreeStruct = generateTreeStruct;
        this.conditionalMemoryType = conditionalMemoryType;
    }

    private static class VisitObject {
        final Object reference;
        final int depth;
        final int identityHash;

        String identifier;
        boolean fieldMemoryVisible = false;
        boolean shallowHeap = false;

        private VisitObject(Object object, int depth) {
            this.reference = object;
            this.depth = depth;
            this.identityHash = System.identityHashCode(object);
        }

        @Override
        public int hashCode() {
            // Identity hash code of the original key
            return identityHash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof VisitObject) {
                Object key = reference;
                Object otherKey = ((VisitObject) obj).reference;
                return key != null
                    && otherKey != null
                    && key == otherKey; // Reference equality
            }
            return false;
        }

        @Override
        public String toString() {
            return "VisitObject{" +
                "reference=" + reference +
                ", depth=" + depth +
                ", identityHash=" + identityHash +
                ", identifier='" + identifier + '\'' +
                '}';
        }
    }

    @Override
    public MemoryUsageReport getMemoryUsage(Object root) {
        Set<String> visitedAddressSet = threadLocalVisitedAddressSet.get();

        try {
            long rootAddress = VMSupport.addressOf(root);
            int rootHC = System.identityHashCode(root);
            String name = root.getClass().getSimpleName()
                + "@" + System.identityHashCode(root);

            List<VisitObject> curLayer = new ArrayList<>();
            List<VisitObject> newLayer = new ArrayList<>();

            VisitObject e = new VisitObject(root, 0);
            e.identifier = name;
            e.shallowHeap = root.getClass().isAnnotationPresent(DefinedMemoryUsage.class);
            visitedAddressSet.add(identifier(e.reference));
            accumulateMemoryUsage(e);

            rootTreeNode = new TreeNode(
                e.identifier,
                conditionalSizeOf(e.reference)
            );
            memoryUsageTree.put(rootTreeNode, new ArrayList<>());

            curLayer.add(e);

            while (!curLayer.isEmpty()) {
                newLayer.clear();
                for (VisitObject next : curLayer) {

                    List<VisitObject> refInThisLayer = handleReferences(next);

                    List<TreeNode> treeNodeList = memoryUsageTree.get(
                        new TreeNode(
                            next.identifier,
                            0L
                        )
                    );
                    Preconditions.checkNotNull(treeNodeList);

                    for (VisitObject ref : refInThisLayer) {

                        if (ref != null) {
                            if (visitedAddressSet.add(identifier(ref.reference))) {

                                accumulateMemoryUsage(ref);

                                TreeNode newTreeNode = new TreeNode(
                                    ref.identifier, conditionalSizeOf(ref.reference)
                                );
                                newTreeNode.fieldMemoryVisible = ref.fieldMemoryVisible;
                                treeNodeList.add(newTreeNode);
                                memoryUsageTree.put(newTreeNode, new ArrayList<>());

                                // Don't exceed the maximum depth.
                                if (ref.depth < maxDepth) {
                                    newLayer.add(ref);
                                }

                            }
                        }
                    }
                }
                curLayer.clear();
                curLayer.addAll(newLayer);
            }

            String treeStruct = "";
            if (generateTreeStruct) {
                collectTreeNode(rootTreeNode, 0);

                StringBuilder treeNodeStringBuilder =
                    new StringBuilder().append("Total usage : ").append(totalSize).append(" bytes\n");
                printTreeNode(treeNodeStringBuilder, rootTreeNode, 0);

                treeStruct = treeNodeStringBuilder.toString();
            }

            return new MemoryUsageReport(fieldSizeMap, totalSize, treeStruct);
        } finally {

            // clear thread local collections.
            visitedAddressSet.clear();
        }

    }

    private long printTreeNode(StringBuilder builder, TreeNode treeNode, int depth) {
        List<TreeNode> treeNodeList = memoryUsageTree.get(treeNode);
        // print
        for (int i = 0; i < 2 * depth; i++) {
            if (i % 2 == 0) {
                builder.append('|');
            } else {
                builder.append(' ');
            }
        }
        builder.append("└ ");
        // builder.append("——");
        builder.append(treeNode.fieldIdentifier);
        builder.append(", shallow: ");
        builder.append(treeNode.memorySize);
        builder.append(" bytes, accumulated:");
        builder.append(treeNode.accumulatedMemorySize);
        builder.append(" bytes\n");

        long accumulatedMemorySize = treeNode.memorySize;
        for (TreeNode childTreeNode : treeNodeList) {
            List<TreeNode> childTreeNodeList = memoryUsageTree.get(childTreeNode);
            if (childTreeNodeList != null) {
                accumulatedMemorySize += printTreeNode(builder, childTreeNode, depth + 1);
            }
        }

        if (treeNode.fieldMemoryVisible) {
            fieldSizeMap.put(treeNode.fieldIdentifier, (int) accumulatedMemorySize);
        }

        return accumulatedMemorySize;
    }

    private long collectTreeNode(TreeNode treeNode, int depth) {
        List<TreeNode> treeNodeList = memoryUsageTree.get(treeNode);

        long accumulatedMemorySize = treeNode.memorySize;
        for (TreeNode childTreeNode : treeNodeList) {
            List<TreeNode> childTreeNodeList = memoryUsageTree.get(childTreeNode);
            if (childTreeNodeList != null) {
                accumulatedMemorySize += collectTreeNode(childTreeNode, depth + 1);
            }
        }

        treeNode.accumulatedMemorySize = accumulatedMemorySize;
        if (treeNode.fieldMemoryVisible) {
            fieldSizeMap.put(treeNode.fieldIdentifier, (int) accumulatedMemorySize);
        }

        return accumulatedMemorySize;
    }

    private void accumulateMemoryUsage(VisitObject visitObject) {
        final String fieldIdentifier = visitObject.identifier;
        try {
            Object object = visitObject.reference;

            final int size = conditionalSizeOf(object);
            totalSize += size;

            if (generateFieldSizeMap && fieldIdentifier != null && !fieldIdentifier.isEmpty()) {
                fieldSizeMap.put(fieldIdentifier, size);
            }
        } catch (Exception e) {
            if (generateFieldSizeMap && fieldIdentifier != null && !fieldIdentifier.isEmpty()) {
                fieldSizeMap.put(fieldIdentifier, -1);
            }
        }

    }

    private int conditionalSizeOf(Object reference) {
        if (reference != null) {

            // check ConditionalMemoryCounter in class level.
            // ignore the class with different memory conditional type.
            if (conditionalMemoryType != null) {
                ConditionalMemoryCounter conditional =
                    reference.getClass().getAnnotation(ConditionalMemoryCounter.class);
                if (conditional != null && conditional.type() != conditionalMemoryType) {
                    return 0;
                }
            }

            DefinedMemoryUsage definedMemoryUsage = reference.getClass().getAnnotation(DefinedMemoryUsage.class);
            if (definedMemoryUsage != null) {

                try {
                    // Check if the object implements the interface
                    if (!(reference instanceof MemoryCountable)) {
                        throw new RuntimeException(
                            "Class " + reference.getClass().getName() + " does not implement MemoryCountable");
                    }

                    // Invoke the method
                    long result = ((MemoryCountable) reference).getMemoryUsage();
                    return (int) result;
                } catch (Throwable t) {
                    throw GeneralUtil.nestedException(t);
                }
            }

        }

        return VMSupport.sizeOf(reference);
    }

    private static boolean isMemoryCountable(Object reference) {
        if (reference instanceof MemoryCountable) {
            try {
                Method method = reference.getClass().getMethod("isMemoryCountable");
                boolean isMemoryCountable = (boolean) method.invoke(reference);
                return isMemoryCountable;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    private List<VisitObject> handleReferences(VisitObject r) {
        List<VisitObject> result = new ArrayList<>();

        Object o = r.reference;
        boolean shallowHeap = r.shallowHeap;
        if (shallowHeap) {
            return result;
        }

        if (o.getClass().isArray() && !o.getClass().getComponentType().isPrimitive()) {
            int c = 0;
            for (Object e : (Object[]) o) {
                if (e != null && isMemoryCountable(e)) {

                    VisitObject newVisitObject = new VisitObject(e, r.depth + 1);
                    newVisitObject.identifier = "[" + c + "]__" + identifier(e);
                    result.add(newVisitObject);

                    c++;
                }
            }
        }

        for (Field f : getAllFields(o.getClass(), useFieldMemoryCounter, conditionalMemoryType)) {
            f.setAccessible(true);
            if (f.getType().isPrimitive()) {
                continue;
            }
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            try {
                Object e = f.get(o);

                if (e != null && isMemoryCountable(e)) {
                    VisitObject visitObject = new VisitObject(e, r.depth + 1);

                    // identifier for field with annotation @FieldMemoryVisible
                    visitObject.identifier = e.getClass().getSimpleName()
                        + "@" + System.identityHashCode(e)
                        + "__" + f.getName();

                    visitObject.fieldMemoryVisible = f.isAnnotationPresent(FieldMemoryVisible.class);
                    visitObject.shallowHeap = f.isAnnotationPresent(ShallowHeap.class)
                        || f.isAnnotationPresent(DefinedMemoryUsage.class)
                        || e.getClass().isAnnotationPresent(DefinedMemoryUsage.class);

                    result.add(visitObject);
                }
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        return result;
    }

    private static Collection<Field> getAllFields(Class<?> klass,
                                                  boolean useFieldMemoryCounter,
                                                  ConditionalMemoryType conditionalMemoryType) {
        List<Field> results = new ArrayList<>();

        for (Field f : klass.getDeclaredFields()) {
            // Ignore static field and check field annotation.
            if (!Modifier.isStatic(f.getModifiers())
                && (!useFieldMemoryCounter || checkAnnotation(f, conditionalMemoryType))) {
                results.add(f);
            }
        }

        Class<?> superKlass = klass;
        while ((superKlass = superKlass.getSuperclass()) != null) {
            for (Field f : superKlass.getDeclaredFields()) {
                // For supper class,
                // ignore static field and check field annotation.
                if (!Modifier.isStatic(f.getModifiers())
                    && (!useFieldMemoryCounter || checkAnnotation(f, conditionalMemoryType))) {
                    results.add(f);
                }
            }
        }

        return results;
    }

    private static boolean checkAnnotation(Field field, ConditionalMemoryType conditionalMemoryType) {
        // check annotation @FieldMemoryCounter
        FieldMemoryCounter annotation = field.getAnnotation(FieldMemoryCounter.class);
        if (annotation != null) {
            return annotation.value();
        }

        ORCFieldMemoryCounter orcAnnotation = field.getAnnotation(ORCFieldMemoryCounter.class);
        if (orcAnnotation != null) {
            return orcAnnotation.value();
        }

        // check ConditionalMemoryCounter
        ConditionalMemoryCounter conditional = field.getAnnotation(ConditionalMemoryCounter.class);
        if (conditionalMemoryType != null && conditional != null && conditional.type() != conditionalMemoryType) {
            return false;
        }

        // no annotation, return true in default.
        return true;
    }

    private static String identifier(Object object) {
        return object.getClass().getSimpleName() + "@" + System.identityHashCode(object);
    }

}

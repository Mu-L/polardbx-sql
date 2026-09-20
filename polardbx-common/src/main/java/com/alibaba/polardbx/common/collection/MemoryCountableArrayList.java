package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.sql.Blob;
import java.util.Collection;
import java.util.Objects;

public class MemoryCountableArrayList<T> extends ReferenceArrayList<T> implements MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableArrayList.class).instanceSize();
    private static final int BLOB_INSTANCE_SIZE = ClassLayout.parseClass(Blob.class).instanceSize();

    @Override
    public int indexOf(final Object k) {
        final Object[] array = a;
        for (int i = 0; i < size; i++) {
            if (Objects.equals(k, array[i])) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(final Object k) {
        final Object[] array = a;
        for (int i = size; i-- != 0; ) {
            if (Objects.equals(k, array[i])) {
                return i;
            }
        }
        return -1;
    }

    public MemoryCountableArrayList() {
        super();
    }

    public MemoryCountableArrayList(Collection<? extends T> c) {
        super(c);
    }

    public MemoryCountableArrayList(int capacity) {
        super(capacity);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
    }
}

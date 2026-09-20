package it.unimi.dsi.fastutil.objects;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryCounter;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class MemoryCountableObject2ObjectArrayMap<K, V> extends AbstractObject2ObjectMap<K, V>
    implements java.io.Serializable, Cloneable, MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableObject2ObjectArrayMap.class).instanceSize();
    private final int KEY_SET_INSTANCE_SIZE = ClassLayout.parseClass(KeySet.class).instanceSize();
    private final int VALUE_COLLECTION_INSTANCE_SIZE = ClassLayout.parseClass(ValuesCollection.class).instanceSize();

    private static final long serialVersionUID = 1L;

    @FieldMemoryCounter(value = false)
    private MemoryCounter<K> kMemoryCounter;
    @FieldMemoryCounter(value = false)
    private MemoryCounter<V> vMemoryCounter;

    private long keyMemoryUsage = 0L;
    private long valueMemoryUsage = 0L;

    /**
     * The keys (valid up to {@link #size}, excluded).
     */
    private transient Object[] key;
    /**
     * The values (parallel to {@link #key}).
     */
    private transient Object[] value;
    /**
     * The number of valid entries in {@link #key} and {@link #value}.
     */
    private int size;
    /**
     * Cached set of keys.
     */
    private transient KeySet keys;
    /**
     * Cached collection of values.
     */
    private transient ValuesCollection values;

    @Override
    public long getMemoryUsage() {
        long size = INSTANCE_SIZE
            + (keys == null ? 0 : KEY_SET_INSTANCE_SIZE)
            + (values == null ? 0 : VALUE_COLLECTION_INSTANCE_SIZE);

        if (key != null) {
            size += VMSupport.align((int) SizeOf.sizeOf(key));
            size += keyMemoryUsage;
        }

        if (value != null) {
            size += VMSupport.align((int) SizeOf.sizeOf(value));
            size += valueMemoryUsage;
        }
        return size;
    }

    public MemoryCountableObject2ObjectArrayMap(final MemoryCounter<K> kMemoryCounter,
                                                final MemoryCounter<V> vMemoryCounter) {
        this.key = new Object[0];
        this.value = new Object[0];
        this.kMemoryCounter = kMemoryCounter;
        this.vMemoryCounter = vMemoryCounter;
    }

    public MemoryCountableObject2ObjectArrayMap(final int capacity, final MemoryCounter<K> kMemoryCounter,
                                                final MemoryCounter<V> vMemoryCounter) {
        this.key = new Object[capacity];
        this.value = new Object[capacity];
        this.kMemoryCounter = kMemoryCounter;
        this.vMemoryCounter = vMemoryCounter;
    }

    @Override
    public FastEntrySet<K, V> object2ObjectEntrySet() {
       throw new UnsupportedOperationException();
    }

    private int findKey(final Object k) {
        final Object[] key = this.key;
        for (int i = size; i-- != 0; ) {
            if (java.util.Objects.equals(key[i], k)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(final Object k) {
        final Object[] key = this.key;
        for (int i = size; i-- != 0; ) {
            if (java.util.Objects.equals(key[i], k)) {
                return (V) value[i];
            }
        }
        return defRetValue;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void clear() {
        for (int i = size; i-- != 0; ) {
            key[i] = null;
            value[i] = null;
        }
        size = 0;
        keyMemoryUsage = 0L;
        valueMemoryUsage = 0L;
    }

    @Override
    public boolean containsKey(final Object k) {
        return findKey(k) != -1;
    }

    @Override
    public boolean containsValue(Object v) {
        for (int i = size; i-- != 0; ) {
            if (java.util.Objects.equals(value[i], v)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(K k, V v) {
        final int oldKey = findKey(k);
        if (oldKey != -1) {
            final V oldValue = (V) value[oldKey];
            value[oldKey] = v;

            valueMemoryUsage -= vMemoryCounter.getMemoryUsage(oldValue);
            valueMemoryUsage += vMemoryCounter.getMemoryUsage(v);

            return oldValue;
        }
        if (size == key.length) {
            // resize
            final Object[] newKey = new Object[size == 0 ? 2 : size * 2];
            final Object[] newValue = new Object[size == 0 ? 2 : size * 2];
            for (int i = size; i-- != 0; ) {
                newKey[i] = key[i];
                newValue[i] = value[i];
            }
            key = newKey;
            value = newValue;
        }
        key[size] = k;
        value[size] = v;

        keyMemoryUsage += kMemoryCounter.getMemoryUsage(k);
        valueMemoryUsage += vMemoryCounter.getMemoryUsage(v);
        size++;
        return defRetValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(final Object k) {
        final int oldPos = findKey(k);
        if (oldPos == -1) {
            return defRetValue;
        }
        final V oldValue = (V) value[oldPos];
        final int tail = size - oldPos - 1;
        System.arraycopy(key, oldPos + 1, key, oldPos, tail);
        System.arraycopy(value, oldPos + 1, value, oldPos, tail);
        size--;
        keyMemoryUsage -= kMemoryCounter.getMemoryUsage((K) key[size]);
        valueMemoryUsage -= vMemoryCounter.getMemoryUsage((V) value[size]);
        key[size] = null;
        value[size] = null;
        return oldValue;
    }

    private final class KeySet extends AbstractObjectSet<K> {
        @Override
        public boolean contains(final Object k) {
            return findKey(k) != -1;
        }

        @Override
        public boolean remove(final Object k) {
            final int oldPos = findKey(k);
            if (oldPos == -1) {
                return false;
            }
            final int tail = size - oldPos - 1;
            System.arraycopy(key, oldPos + 1, key, oldPos, tail);
            System.arraycopy(value, oldPos + 1, value, oldPos, tail);
            size--;
            MemoryCountableObject2ObjectArrayMap.this.key[size] = null;
            MemoryCountableObject2ObjectArrayMap.this.value[size] = null;
            return true;
        }

        @Override
        public ObjectIterator<K> iterator() {
            return new ObjectIterator<K>() {
                int pos = 0;

                @Override
                public boolean hasNext() {
                    return pos < size;
                }

                @Override
                @SuppressWarnings("unchecked")
                public K next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return (K) key[pos++];
                }

                @Override
                public void remove() {
                    if (pos == 0) {
                        throw new IllegalStateException();
                    }
                    final int tail = size - pos;
                    System.arraycopy(key, pos, key, pos - 1, tail);
                    System.arraycopy(value, pos, value, pos - 1, tail);
                    size--;
                    pos--;
                    MemoryCountableObject2ObjectArrayMap.this.key[size] = null;
                    MemoryCountableObject2ObjectArrayMap.this.value[size] = null;
                }

                @Override
                @SuppressWarnings("unchecked")
                public void forEachRemaining(final Consumer<? super K> action) {
                    // Hoist containing class field ref into local
                    final int max = size;
                    while (pos < max) {
                        action.accept((K) key[pos++]);
                    }
                }
                // TODO either override skip or extend from AbstractIndexBasedIterator.
            };
        }

        final class KeySetSpliterator extends ObjectSpliterators.EarlyBindingSizeIndexBasedSpliterator<K>
            implements ObjectSpliterator<K> {
            KeySetSpliterator(int pos, int maxPos) {
                super(pos, maxPos);
            }

            @Override
            public int characteristics() {
                return ObjectSpliterators.SET_SPLITERATOR_CHARACTERISTICS | java.util.Spliterator.SUBSIZED
                    | java.util.Spliterator.ORDERED;
            }

            @Override
            @SuppressWarnings("unchecked")
            protected final K get(int location) {
                return (K) key[location];
            }

            @Override
            protected final KeySetSpliterator makeForSplit(int pos, int maxPos) {
                return new KeySetSpliterator(pos, maxPos);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void forEachRemaining(final Consumer<? super K> action) {
                // Hoist containing class field ref into local
                final int max = size;
                while (pos < max) {
                    action.accept((K) key[pos++]);
                }
            }
        }

        @Override
        public ObjectSpliterator<K> spliterator() {
            return new KeySetSpliterator(0, size);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void forEach(Consumer<? super K> action) {
            // Hoist containing class field ref into local
            for (int i = 0, max = size; i < max; ++i) {
                action.accept((K) key[i]);
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public void clear() {
            MemoryCountableObject2ObjectArrayMap.this.clear();
        }
    }

    @Override
    public ObjectSet<K> keySet() {
        if (keys == null) {
            keys = new KeySet();
        }
        return keys;
    }

    private final class ValuesCollection extends AbstractObjectCollection<V> {
        @Override
        public boolean contains(final Object v) {
            return containsValue(v);
        }

        @Override
        public it.unimi.dsi.fastutil.objects.ObjectIterator<V> iterator() {
            return new it.unimi.dsi.fastutil.objects.ObjectIterator<V>() {
                int pos = 0;

                @Override
                public boolean hasNext() {
                    return pos < size;
                }

                @Override
                @SuppressWarnings("unchecked")
                public V next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return (V) value[pos++];
                }

                @Override
                public void remove() {
                    if (pos == 0) {
                        throw new IllegalStateException();
                    }
                    final int tail = size - pos;
                    System.arraycopy(key, pos, key, pos - 1, tail);
                    System.arraycopy(value, pos, value, pos - 1, tail);
                    size--;
                    pos--;
                    MemoryCountableObject2ObjectArrayMap.this.key[size] = null;
                    MemoryCountableObject2ObjectArrayMap.this.value[size] = null;
                }

                @Override
                @SuppressWarnings("unchecked")
                public void forEachRemaining(final Consumer<? super V> action) {
                    // Hoist containing class field ref into local
                    final int max = size;
                    while (pos < max) {
                        action.accept((V) value[pos++]);
                    }
                }
                // TODO either override skip or extend from AbstractIndexBasedIterator.
            };
        }

        final class ValuesSpliterator
            extends it.unimi.dsi.fastutil.objects.ObjectSpliterators.EarlyBindingSizeIndexBasedSpliterator<V>
            implements it.unimi.dsi.fastutil.objects.ObjectSpliterator<V> {
            ValuesSpliterator(int pos, int maxPos) {
                super(pos, maxPos);
            }

            @Override
            public int characteristics() {
                return it.unimi.dsi.fastutil.objects.ObjectSpliterators.COLLECTION_SPLITERATOR_CHARACTERISTICS
                    | java.util.Spliterator.SUBSIZED | java.util.Spliterator.ORDERED;
            }

            @Override
            @SuppressWarnings("unchecked")
            protected final V get(int location) {
                return (V) value[location];
            }

            @Override
            protected final ValuesSpliterator makeForSplit(int pos, int maxPos) {
                return new ValuesSpliterator(pos, maxPos);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void forEachRemaining(final Consumer<? super V> action) {
                // Hoist containing class field ref into local
                final int max = size;
                while (pos < max) {
                    action.accept((V) value[pos++]);
                }
            }
        }

        @Override
        public it.unimi.dsi.fastutil.objects.ObjectSpliterator<V> spliterator() {
            return new ValuesSpliterator(0, size);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void forEach(Consumer<? super V> action) {
            // Hoist containing class field ref into local
            for (int i = 0, max = size; i < max; ++i) {
                action.accept((V) value[i]);
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public void clear() {
            MemoryCountableObject2ObjectArrayMap.this.clear();
        }
    }

    @Override
    public ObjectCollection<V> values() {
        if (values == null) {
            values = new ValuesCollection();
        }
        return values;
    }

    /**
     * Returns a deep copy of this map.
     *
     * <p>This method performs a deep copy of this hash map; the data stored in the
     * map, however, is not cloned. Note that this makes a difference only for object keys.
     *
     * @return a deep copy of this map.
     */
    @Override
    @SuppressWarnings("unchecked")
    public MemoryCountableObject2ObjectArrayMap<K, V> clone() {
        MemoryCountableObject2ObjectArrayMap<K, V> c;
        try {
            c = (MemoryCountableObject2ObjectArrayMap<K, V>) super.clone();
        } catch (CloneNotSupportedException cantHappen) {
            throw new InternalError();
        }
        c.key = key.clone();
        c.value = value.clone();
        c.keys = null;
        c.values = null;
        return c;
    }

    private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
        s.defaultWriteObject();
        for (int i = 0, max = size; i < max; i++) {
            s.writeObject(key[i]);
            s.writeObject(value[i]);
        }
    }

    private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        key = new Object[size];
        value = new Object[size];
        for (int i = 0; i < size; i++) {
            key[i] = s.readObject();
            value[i] = s.readObject();
        }
    }
}


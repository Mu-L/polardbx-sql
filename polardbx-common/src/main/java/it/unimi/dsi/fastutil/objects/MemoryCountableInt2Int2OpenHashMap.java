package it.unimi.dsi.fastutil.objects;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.AbstractInt2IntMap;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Map;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

public class MemoryCountableInt2Int2OpenHashMap extends AbstractInt2IntMap
    implements java.io.Serializable, Cloneable, Hash, MemoryCountable {
    private static final long serialVersionUID = 0L;
    private static final boolean ASSERTS = false;
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableInt2Int2OpenHashMap.class).instanceSize();
    /**
     * The array of keys.
     */
    protected transient int[] key;
    /**
     * The array of values.
     */
    protected transient int[] value;
    /**
     * The mask for wrapping a position counter.
     */
    protected transient int mask;
    /**
     * Whether this map contains the key zero.
     */
    protected transient boolean containsNullKey;
    /**
     * The current table size.
     */
    protected transient int n;
    /**
     * Threshold after which we rehash. It must be the table size times {@link #f}.
     */
    protected transient int maxFill;
    /**
     * We never resize below this threshold, which is the construction-time {#n}.
     */
    protected final transient int minN;
    /**
     * Number of entries in the set (including the key zero, if present).
     */
    protected int size;
    /**
     * The acceptable load factor.
     */
    protected final float f;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + FastMemoryCounter.sizeOf(key) + FastMemoryCounter.sizeOf(value);
    }

    public MemoryCountableInt2Int2OpenHashMap(final int expected, final float f) {
        if (f <= 0 || f >= 1) {
            throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than 1");
        }
        if (expected < 0) {
            throw new IllegalArgumentException("The expected number of elements must be nonnegative");
        }
        this.f = f;
        minN = n = arraySize(expected, f);
        mask = n - 1;
        maxFill = maxFill(n, f);
        key = new int[n + 1];
        value = new int[n + 1];
    }

    /**
     * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
     *
     * @param expected the expected number of elements in the hash map.
     */
    public MemoryCountableInt2Int2OpenHashMap(final int expected) {
        this(expected, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new hash map with initial expected {@link Hash#DEFAULT_INITIAL_SIZE} entries
     * and {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
     */
    public MemoryCountableInt2Int2OpenHashMap() {
        this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
    }

    private int realSize() {
        return containsNullKey ? size - 1 : size;
    }

    private void ensureCapacity(final int capacity) {
        final int needed = arraySize(capacity, f);
        if (needed > n) {
            rehash(needed);
        }
    }

    private void tryCapacity(final long capacity) {
        final int needed =
            (int) Math.min(1 << 30, Math.max(2, HashCommon.nextPowerOfTwo((long) Math.ceil(capacity / f))));
        if (needed > n) {
            rehash(needed);
        }
    }

    private int removeEntry(final int pos) {
        final int oldValue = value[pos];
        size--;
        shiftKeys(pos);
        if (n > minN && size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE) {
            rehash(n / 2);
        }
        return oldValue;
    }

    private int removeNullEntry() {
        containsNullKey = false;
        final int oldValue = value[n];
        size--;
        if (n > minN && size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE) {
            rehash(n / 2);
        }
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends Integer> m) {
        if (f <= .5) {
            ensureCapacity(m.size()); // The resulting map will be sized for m.size() elements
        } else {
            tryCapacity(
                size() + m.size()); // The resulting map will be tentatively sized for size() + m.size() elements
        }
        super.putAll(m);
    }

    private int find(final int k) {
        if (((k) == (0))) {
            return containsNullKey ? n : -(n + 1);
        }
        int curr;
        final int[] key = this.key;
        int pos;
        // The starting point.
        if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k))) & mask]) == (0))) {
            return -(pos + 1);
        }
        if (((k) == (curr))) {
            return pos;
        }
        // There's always an unused entry.
        while (true) {
            if (((curr = key[pos = (pos + 1) & mask]) == (0))) {
                return -(pos + 1);
            }
            if (((k) == (curr))) {
                return pos;
            }
        }
    }

    private void insert(final int pos, final int k, final int v) {
        if (pos == n) {
            containsNullKey = true;
        }
        key[pos] = k;
        value[pos] = v;
        if (size++ >= maxFill) {
            rehash(arraySize(size + 1, f));
        }
        if (ASSERTS) {
            checkTable();
        }
    }

    private void checkTable() {
    }

    @Override
    public int put(final int k, final int v) {
        final int pos = find(k);
        if (pos < 0) {
            insert(-pos - 1, k, v);
            return defRetValue;
        }
        final int oldValue = value[pos];
        value[pos] = v;
        return oldValue;
    }

    private int addToValue(final int pos, final int incr) {
        final int oldValue = value[pos];
        value[pos] = oldValue + incr;
        return oldValue;
    }

    /**
     * Adds an increment to value currently associated with a key.
     *
     * <p>Note that this method respects the {@linkplain #defaultReturnValue() default return value} semantics: when
     * called with a key that does not currently appears in the map, the key
     * will be associated with the default return value plus
     * the given increment.
     *
     * @param k the key.
     * @param incr the increment.
     * @return the old value, or the {@linkplain #defaultReturnValue() default return value} if no value was present for the given key.
     */
    public int addTo(final int k, final int incr) {
        int pos;
        if (((k) == (0))) {
            if (containsNullKey) {
                return addToValue(n, incr);
            }
            pos = n;
            containsNullKey = true;
        } else {
            int curr;
            final int[] key = this.key;
            // The starting point.
            if (!((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k))) & mask]) == (0))) {
                if (((curr) == (k))) {
                    return addToValue(pos, incr);
                }
                while (!((curr = key[pos = (pos + 1) & mask]) == (0))) {
                    if (((curr) == (k))) {
                        return addToValue(pos, incr);
                    }
                }
            }
        }
        key[pos] = k;
        value[pos] = defRetValue + incr;
        if (size++ >= maxFill) {
            rehash(arraySize(size + 1, f));
        }
        if (ASSERTS) {
            checkTable();
        }
        return defRetValue;
    }

    /**
     * Shifts left entries with the specified hash code, starting at the specified position,
     * and empties the resulting free entry.
     *
     * @param pos a starting position.
     */
    protected final void shiftKeys(int pos) {
        // Shift entries with the same hash.
        int last, slot;
        int curr;
        final int[] key = this.key;
        for (; ; ) {
            pos = ((last = pos) + 1) & mask;
            for (; ; ) {
                if (((curr = key[pos]) == (0))) {
                    key[last] = (0);
                    return;
                }
                slot = (it.unimi.dsi.fastutil.HashCommon.mix((curr))) & mask;
                if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
                    break;
                }
                pos = (pos + 1) & mask;
            }
            key[last] = curr;
            value[last] = value[pos];
        }
    }

    @Override

    public int remove(final int k) {
        if (((k) == (0))) {
            if (containsNullKey) {
                return removeNullEntry();
            }
            return defRetValue;
        }
        int curr;
        final int[] key = this.key;
        int pos;
        // The starting point.
        if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k))) & mask]) == (0))) {
            return defRetValue;
        }
        if (((k) == (curr))) {
            return removeEntry(pos);
        }
        while (true) {
            if (((curr = key[pos = (pos + 1) & mask]) == (0))) {
                return defRetValue;
            }
            if (((k) == (curr))) {
                return removeEntry(pos);
            }
        }
    }

    @Override

    public int get(final int k) {
        if (((k) == (0))) {
            return containsNullKey ? value[n] : defRetValue;
        }
        int curr;
        final int[] key = this.key;
        int pos;
        // The starting point.
        if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k))) & mask]) == (0))) {
            return defRetValue;
        }
        if (((k) == (curr))) {
            return value[pos];
        }
        // There's always an unused entry.
        while (true) {
            if (((curr = key[pos = (pos + 1) & mask]) == (0))) {
                return defRetValue;
            }
            if (((k) == (curr))) {
                return value[pos];
            }
        }
    }

    @Override

    public boolean containsKey(final int k) {
        if (((k) == (0))) {
            return containsNullKey;
        }
        int curr;
        final int[] key = this.key;
        int pos;
        // The starting point.
        if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k))) & mask]) == (0))) {
            return false;
        }
        if (((k) == (curr))) {
            return true;
        }
        // There's always an unused entry.
        while (true) {
            if (((curr = key[pos = (pos + 1) & mask]) == (0))) {
                return false;
            }
            if (((k) == (curr))) {
                return true;
            }
        }
    }

    @Override
    public boolean containsValue(final int v) {
        final int value[] = this.value;
        final int key[] = this.key;
        if (containsNullKey && ((value[n]) == (v))) {
            return true;
        }
        for (int i = n; i-- != 0; ) {
            if (!((key[i]) == (0)) && ((value[i]) == (v))) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getOrDefault(final int k, final int defaultValue) {
        if (((k) == (0))) {
            return containsNullKey ? value[n] : defaultValue;
        }
        int curr;
        final int[] key = this.key;
        int pos;
        // The starting point.
        if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k))) & mask]) == (0))) {
            return defaultValue;
        }
        if (((k) == (curr))) {
            return value[pos];
        }
        // There's always an unused entry.
        while (true) {
            if (((curr = key[pos = (pos + 1) & mask]) == (0))) {
                return defaultValue;
            }
            if (((k) == (curr))) {
                return value[pos];
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int putIfAbsent(final int k, final int v) {
        final int pos = find(k);
        if (pos >= 0) {
            return value[pos];
        }
        insert(-pos - 1, k, v);
        return defRetValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override

    public boolean remove(final int k, final int v) {
        if (((k) == (0))) {
            if (containsNullKey && ((v) == (value[n]))) {
                removeNullEntry();
                return true;
            }
            return false;
        }
        int curr;
        final int[] key = this.key;
        int pos;
        // The starting point.
        if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k))) & mask]) == (0))) {
            return false;
        }
        if (((k) == (curr)) && ((v) == (value[pos]))) {
            removeEntry(pos);
            return true;
        }
        while (true) {
            if (((curr = key[pos = (pos + 1) & mask]) == (0))) {
                return false;
            }
            if (((k) == (curr)) && ((v) == (value[pos]))) {
                removeEntry(pos);
                return true;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean replace(final int k, final int oldValue, final int v) {
        final int pos = find(k);
        if (pos < 0 || !((oldValue) == (value[pos]))) {
            return false;
        }
        value[pos] = v;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int replace(final int k, final int v) {
        final int pos = find(k);
        if (pos < 0) {
            return defRetValue;
        }
        final int oldValue = value[pos];
        value[pos] = v;
        return oldValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeIfAbsent(final int k, final java.util.function.IntUnaryOperator mappingFunction) {
        java.util.Objects.requireNonNull(mappingFunction);
        final int pos = find(k);
        if (pos >= 0) {
            return value[pos];
        }
        final int newValue = mappingFunction.applyAsInt(k);
        insert(-pos - 1, k, newValue);
        return newValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeIfAbsentNullable(final int k,
                                       final java.util.function.IntFunction<? extends Integer> mappingFunction) {
        java.util.Objects.requireNonNull(mappingFunction);
        final int pos = find(k);
        if (pos >= 0) {
            return value[pos];
        }
        final Integer newValue = mappingFunction.apply(k);
        if (newValue == null) {
            return defRetValue;
        }
        final int v = (newValue).intValue();
        insert(-pos - 1, k, v);
        return v;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeIfPresent(final int k,
                                final java.util.function.BiFunction<? super Integer, ? super Integer, ? extends Integer> remappingFunction) {
        java.util.Objects.requireNonNull(remappingFunction);
        final int pos = find(k);
        if (pos < 0) {
            return defRetValue;
        }
        final Integer newValue = remappingFunction.apply(Integer.valueOf(k), Integer.valueOf(value[pos]));
        if (newValue == null) {
            if (((k) == (0))) {
                removeNullEntry();
            } else {
                removeEntry(pos);
            }
            return defRetValue;
        }
        return value[pos] = (newValue).intValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compute(final int k,
                       final java.util.function.BiFunction<? super Integer, ? super Integer, ? extends Integer> remappingFunction) {
        java.util.Objects.requireNonNull(remappingFunction);
        final int pos = find(k);
        final Integer newValue =
            remappingFunction.apply(Integer.valueOf(k), pos >= 0 ? Integer.valueOf(value[pos]) : null);
        if (newValue == null) {
            if (pos >= 0) {
                if (((k) == (0))) {
                    removeNullEntry();
                } else {
                    removeEntry(pos);
                }
            }
            return defRetValue;
        }
        int newVal = (newValue).intValue();
        if (pos < 0) {
            insert(-pos - 1, k, newVal);
            return newVal;
        }
        return value[pos] = newVal;
    }

    @Override
    public void clear() {
        if (size == 0) {
            return;
        }
        size = 0;
        containsNullKey = false;
        Arrays.fill(key, (0));
    }

    @Override
    public ObjectSet<Entry> int2IntEntrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    protected void rehash(final int newN) {
        final int key[] = this.key;
        final int value[] = this.value;
        final int mask = newN - 1; // Note that this is used by the hashing macro
        final int newKey[] = new int[newN + 1];
        final int newValue[] = new int[newN + 1];
        int i = n, pos;
        for (int j = realSize(); j-- != 0; ) {
            while (((key[--i]) == (0)))
                ;
            if (!((newKey[pos = (it.unimi.dsi.fastutil.HashCommon.mix((key[i]))) & mask]) == (0))) {
                while (!((newKey[pos = (pos + 1) & mask]) == (0)))
                    ;
            }
            newKey[pos] = key[i];
            newValue[pos] = value[i];
        }
        newValue[newN] = value[n];
        n = newN;
        this.mask = mask;
        maxFill = maxFill(n, f);
        this.key = newKey;
        this.value = newValue;
    }

    /**
     * Returns a hash code for this map.
     * <p>
     * This method overrides the generic method provided by the superclass.
     * Since {@code equals()} is not overriden, it is important
     * that the value returned by this method is the same value as
     * the one returned by the overriden method.
     *
     * @return a hash code for this map.
     */
    @Override
    public int hashCode() {
        int h = 0;
        for (int j = realSize(), i = 0, t = 0; j-- != 0; ) {
            while (((key[i]) == (0))) {
                i++;
            }
            t = (key[i]);
            t ^= (value[i]);
            h += t;
            i++;
        }
        // Zero / null keys have hash zero.
        if (containsNullKey) {
            h += (value[n]);
        }
        return h;
    }
}

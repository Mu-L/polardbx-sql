package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.apache.orc.OrcProto;

/**
 * Interface for fast access to ORC Stream Index information.
 */
public interface FastStreamIndex extends MemoryCountable {

    /**
     * Get the size of the stream list.
     *
     * @return the size of the stream list.
     */
    int getStreamListSize();

    /**
     * Get the kind of the specified stream index.
     *
     * @param streamIndex the index of the stream.
     * @return the kind of the stream.
     */
    OrcProto.Stream.Kind getKind(int streamIndex);

    /**
     * Get the column ID associated with the specified stream index.
     *
     * @param streamIndex the index of the stream.
     * @return the column ID.
     */
    int getColumnId(int streamIndex);

    /**
     * Get the length of the specified stream index.
     *
     * @param streamIndex the index of the stream.
     * @return the length of the stream.
     */
    long getLength(int streamIndex);
}

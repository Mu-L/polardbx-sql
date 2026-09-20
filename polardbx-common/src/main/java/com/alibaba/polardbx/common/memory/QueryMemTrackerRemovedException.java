package com.alibaba.polardbx.common.memory;

/**
 * Exception thrown when attempting to access a query memory tracker that has been removed.
 *
 * @author xuzhong.xz
 */
public class QueryMemTrackerRemovedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueryMemTrackerRemovedException() {
        super();
    }

    public QueryMemTrackerRemovedException(String message) {
        super(message);
    }

    public QueryMemTrackerRemovedException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryMemTrackerRemovedException(Throwable cause) {
        super(cause);
    }
}
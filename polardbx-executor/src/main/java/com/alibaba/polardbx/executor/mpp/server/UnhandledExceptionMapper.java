package com.alibaba.polardbx.executor.mpp.server;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Catches any exception not handled by a more specific {@link ExceptionMapper}
 * (e.g. airlift's {@code ParsingExceptionMapper}) and logs the full stack
 * before returning a clean 500, so that worker-side HTTP 500s no longer
 * silently vanish. Without this, Jersey logs via java.util.logging which is
 * not bridged to logback, so the cause is lost.
 */
@Provider
public class UnhandledExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Logger log = LoggerFactory.getLogger(UnhandledExceptionMapper.class);

    @Override
    public Response toResponse(Throwable t) {
        // WebApplicationException already carries an HTTP Response and is normally
        // dispatched by Jersey itself without invoking mappers; log only the
        // unexpected ones.
        log.error("Unhandled exception in MPP HTTP endpoint", t);
        return Response.serverError().entity("Internal error").build();
    }
}

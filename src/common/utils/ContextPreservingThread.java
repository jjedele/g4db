package common.utils;

import org.apache.logging.log4j.ThreadContext;

import java.util.Map;

/**
 * Simple thread extension that inherits the Log4J thread context from the parent thread-
 *
 * Theoretically Log4J has its own mechanism for this, but we don't get it to work.
 */
public class ContextPreservingThread extends Thread {

    private final Map<String, String> parentContext = ThreadContext.getContext();

    protected void setUpThreadContext() {
        ThreadContext.putAll(parentContext);
    }

}

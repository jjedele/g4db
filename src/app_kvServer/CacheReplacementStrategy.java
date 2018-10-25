package app_kvServer;

/**
 * A replacement strategy determines which values will be ejected
 * from the cache when the capacity limit is reached.
 */
public enum CacheReplacementStrategy {

    /**
     * First In, First Out
     *
     * Elements are ejected from the cache in the order they have
     * been added.
     */
    FIFO,

    /**
     * Least Recently Used
     *
     * The element of which the access time lies the furthest in the
     * past is ejected first.
     */
    LRU,

    /**
     * Least Frequently Used
     *
     * The element which has been used the most infrequently is
     * ejected first.
     */
    LFU

}

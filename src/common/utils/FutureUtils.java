package common.utils;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Some utility functions to streamline working with {@link CompletableFuture}s.
 */
public final class FutureUtils {

    private FutureUtils() {}

    /**
     * Return a future that failed with given exception.
     *
     * @param throwable The Throwable to fail the future with.
     * @return Failed future.
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> failedResult = new CompletableFuture<>();
        failedResult.completeExceptionally(throwable);
        return failedResult;
    }

    /**
     * Return a future that completes when all given futures complete.
     *
     * Wrapper around {@link CompletableFuture#allOf(CompletableFuture[])} to work with
     * arbitrary collections of futures and not just arrays.
     *
     * @param futures Collection of futures to combine.
     * @param <T> Result type of the combined futures.
     * @return Combined future.
     */
    public static <T> CompletableFuture<Void> allOf(Collection<CompletableFuture<T>> futures) {
        CompletableFuture[] futureArray = futures.toArray(new CompletableFuture[] {});
        return CompletableFuture.allOf(futureArray);
    }

}

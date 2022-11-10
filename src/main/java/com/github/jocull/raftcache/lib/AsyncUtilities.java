package com.github.jocull.raftcache.lib;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class AsyncUtilities {
    private AsyncUtilities() {
    }

    // Taken from https://stackoverflow.com/a/30026710/97964 and modified slightly
    public static<T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> com) {
        return CompletableFuture.allOf(com.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> com.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toCollection(ArrayList::new))
                );
    }
}

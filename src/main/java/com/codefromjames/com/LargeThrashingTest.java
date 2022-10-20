package com.codefromjames.com;

import com.codefromjames.com.lib.KeyPayload;
import com.codefromjames.com.lib.PartitionMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LargeThrashingTest implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LargeThrashingTest.class);
    private static final Random RANDOM = new Random();

    private final int valueSizeBytes;

    private final PartitionMap partitionMap;
    private final List<String> keySet;
    private final Map<String, String> ackedKeyHashes = new HashMap<>();

    public LargeThrashingTest(PartitionMap partitionMap, int keyCount, int valueSizeBytes) {
        if (keyCount < 1) {
            throw new IllegalArgumentException("Key count must be >= 1");
        }
        if (valueSizeBytes < 0) {
            throw new IllegalArgumentException("Value byte size must be >= 0");
        }

        this.partitionMap = partitionMap;
        this.valueSizeBytes = valueSizeBytes;

        this.keySet = IntStream.range(0, keyCount)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toList());
    }

    @Override
    public void run() {
        final MutableInt counter = new MutableInt(0);
        final Deque<String> keyRotation = new ArrayDeque<>(keySet);
        while (true) {
            if (keyRotation.isEmpty()) {
                keyRotation.addAll(keySet);
            }
            final LargeObject largeObject = new LargeObject(keyRotation.pop(), valueSizeBytes);
            try {
                partitionMap.getData(largeObject.key)
                        .map(KeyPayload::getData)
                        .map(x -> x.get("data"))
                        .map(DigestUtils::sha1Hex)
                        .ifPresent(currentHash -> {
                            final String ackedHash = ackedKeyHashes.get(largeObject.key);
                            if (ackedHash != null
                                    && !ackedHash.equals(currentHash)) {
                                LOGGER.warn("Current key {} hash {} does not match {}", largeObject.key, currentHash, ackedHash);
                            }
                        });

                partitionMap.setData(new KeyPayload(largeObject.key, Map.of("data", largeObject.blob)));
                LOGGER.debug("Wrote #{}, {} w/ hash {}", counter.getAndIncrement(), largeObject.key, largeObject.hash);
                ackedKeyHashes.put(largeObject.key, largeObject.hash);
            } catch (Exception ex) {
                LOGGER.error("Operation failed", ex);
            }
//            try {
//                Thread.sleep(250);
//            } catch (InterruptedException ex) {
//                LOGGER.info("Interrupted", ex);
//                return;
//            }
        }
    }

    private static class LargeObject {
        final String key;
        final byte[] blob;
        final String hash;

        LargeObject(String key, int valueSizeBytes) {
            this.key = key;
            blob = new byte[valueSizeBytes];
            RANDOM.nextBytes(blob);
            this.hash = DigestUtils.sha1Hex(blob);
        }
    }
}

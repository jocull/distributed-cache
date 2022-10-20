package com.codefromjames.com.lib;

import com.codefromjames.com.lib.data.KeyPayload;
import com.codefromjames.com.lib.data.Partition;
import com.codefromjames.com.lib.data.PartitionMap;
import com.codefromjames.com.lib.event.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class PartitionMapTest {
    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = new EventBus();
    }

    @Test
    void testPartitionSize() {
        final PartitionMap pm1 = new PartitionMap(eventBus, 16);
        final PartitionMap pm2 = new PartitionMap(eventBus, 256);
        final PartitionMap pm3 = new PartitionMap(eventBus, 1024);
        final PartitionMap pm4 = new PartitionMap(eventBus, 1);

        assertEquals(16, pm1.getPartitionCount());
        assertEquals(256, pm2.getPartitionCount());
        assertEquals(1024, pm3.getPartitionCount());
        assertEquals(1, pm4.getPartitionCount());

        assertThrows(IllegalArgumentException.class, () -> new PartitionMap(eventBus, 0));
        assertThrows(IllegalArgumentException.class, () -> new PartitionMap(eventBus, -10));
    }

    @Test
    void testPartitionForKey() {
        final PartitionMap pm = new PartitionMap(eventBus, 16);
        assertEquals(new Partition(2), pm.partitionForKey("hello"));
        assertEquals(new Partition(11), pm.partitionForKey("542342342342342342344"));
    }

    @Test
    void testPartitionDistribution() {
        final PartitionMap pm = new PartitionMap(eventBus, 256);
        final Map<Partition, List<Partition>> keys = IntStream.range(0, 10000)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(pm::partitionForKey)
                .collect(Collectors.groupingBy(Function.identity()));

        keys.forEach((k, v) -> {
            assertTrue(v.size() > 10, "Key distribution @ " + k + " weak w/ size " + v.size());
        });
    }

    @Test
    void testDataGetSet() {
        final Random random = new Random();
        final PartitionMap pm = new PartitionMap(eventBus, 256);
        final List<KeyPayload> payloads = IntStream.range(0, 1000)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(key -> {
                    final byte[] data = new byte[32];
                    random.nextBytes(data);
                    return new KeyPayload(key, Map.of("data", data));
                })
                .collect(Collectors.toList());

        payloads.forEach(pm::setData);
        payloads.forEach(p -> {
            final String key = p.getKey();
            final KeyPayload payload = pm.getData(key).orElseThrow();
            assertEquals(p, payload);
        });
    }
}
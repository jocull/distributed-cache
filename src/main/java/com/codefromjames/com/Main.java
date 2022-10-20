package com.codefromjames.com;

import com.codefromjames.com.lib.PartitionMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("App start");
        SpringApplication.run(Main.class, args);
        LOGGER.info("App exiting");
    }

    @Override
    public void run(String... args) throws Exception {
        final PartitionMap partitionMap = new PartitionMap(1024);
        IntStream.range(0, 8)
                .mapToObj(i -> {
                    Thread t = new Thread(new LargeThrashingTest(partitionMap, 50, 1024 * 10 * (i + 1)));
                    t.setName("data-pusher-" + i);
                    t.start();
                    return t;
                })
                .collect(Collectors.toList())
                .forEach(t -> {
                    try {
                        t.join();
                    } catch (InterruptedException ex) {
                        LOGGER.info("Interrupted", ex);
                    }
                });
    }
}
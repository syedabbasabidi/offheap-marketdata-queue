package com.abidi.runner;

import com.abidi.consumer.SPSCQueueConsumer;
import com.abidi.producer.SPSCQueueProducer;
import com.abidi.queue.SPSCCircularQueue;
import com.abidi.queue.SPSCLockFreeCircularQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPSCLatencyPerfMain {

    private static final Logger LOG = LoggerFactory.getLogger(SPSCLatencyPerfMain.class);
    private static final int DEFAULT_QUEUE_SIZE = 1 << 16;

    public static void main(String[] args) throws InterruptedException {
        int queueSize = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_QUEUE_SIZE;
        validatePowerOfTwo(queueSize);

        SPSCCircularQueue queue = new SPSCLockFreeCircularQueue(queueSize);

        SPSCQueueProducer producer = new SPSCQueueProducer(queue);
        SPSCQueueConsumer consumer = new SPSCQueueConsumer(queue);

        Thread producerThread = new Thread(producer, "spsc-producer");
        Thread consumerThread = new Thread(consumer, "spsc-consumer");


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown requested. Stopping producer and consumer...");
            try {
                producerThread.join(5_000);
                consumerThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "spsc-shutdown"));

        LOG.info("Starting SPSC latency harness with queueSize={}", queueSize);
        producerThread.start();
        consumerThread.start();

        producerThread.join();
        consumerThread.join();
    }

    private static void validatePowerOfTwo(int size) {
        if (size <= 0 || Integer.bitCount(size) != 1) {
            throw new IllegalArgumentException("queue size must be a power of two");
        }
    }
}


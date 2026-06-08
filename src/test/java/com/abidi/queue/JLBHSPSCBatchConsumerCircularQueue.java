package com.abidi.queue;

import com.abidi.producer.SPSCQueueProducer;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JLBHSPSCBatchConsumerCircularQueue implements JLBHTask {

    public static final int BATCH_SIZE = 1000;
    public static final int ITERATIONS = 500_000;
    public static final int THROUGHPUT = 10_000;
    public static final int RUNS = 5;
    public static final int WARM_UP_ITERATIONS = 100;
    public static final int QUEUE_SIZE = 65536;
    private JLBH jlbh;
    private SPSCCircularQueue queue;
    private Thread producerThread;
    private final long[] batch = new long[BATCH_SIZE];
    private final JLBHSPSCQueueType queueType;

    private static final Logger LOG = LoggerFactory.getLogger(JLBHSPSCBatchConsumerCircularQueue.class);

    public JLBHSPSCBatchConsumerCircularQueue(JLBHSPSCQueueType queueType) {
        this.queueType = queueType;
    }

    public static void main(String[] args) {
        JLBHSPSCQueueType queueType = JLBHSPSCQueueType.fromArg(args, JLBHSPSCQueueType.LOCKFREE);
        LOG.info("Starting batch consumer-side JLBH with queueType={} queueSize={} batchSize={}",
                queueType, QUEUE_SIZE, BATCH_SIZE);

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS).iterations(ITERATIONS)
                .throughput(THROUGHPUT).runs(RUNS).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHSPSCBatchConsumerCircularQueue(queueType));

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        queue = queueType.create(QUEUE_SIZE);
        SPSCQueueProducer producer = new SPSCQueueProducer(queue, true);
        producerThread = new Thread(producer::run, "JLBH Producer");
        producerThread.start();
    }

    @Override
    public void run(long startTimeNS) {
        while (!queue.batchGet(batch)) {
            Thread.onSpinWait();
        }
        jlbh.sampleNanos(System.nanoTime() - startTimeNS);
    }

    @Override
    public void complete() {
        producerThread.interrupt();
    }
}

package com.abidi.queue;

import com.abidi.consumer.SPSCQueueConsumer;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class JLBHSPSCBatchProducerCircularQueue implements JLBHTask {

    public static final int BATCH_SIZE = 1000;
    public static final int ITERATIONS = 5_000_00;
    public static final int THROUGHPUT = 10_000;
    public static final int RUNS = 5;
    public static final int WARM_UP_ITERATIONS = 100;
    public static final int QUEUE_SIZE = 65536;
    private JLBH jlbh;
    private SPSCCircularQueue queue;
    private Thread consumerThread;
    private final long[] batch = new long[BATCH_SIZE];
    private final JLBHSPSCQueueType queueType;

    private static final Logger LOG = LoggerFactory.getLogger(JLBHSPSCBatchProducerCircularQueue.class);

    public JLBHSPSCBatchProducerCircularQueue(JLBHSPSCQueueType queueType) {
        this.queueType = queueType;
    }

    public static void main(String[] args) {
        JLBHSPSCQueueType queueType = JLBHSPSCQueueType.fromArg(args, JLBHSPSCQueueType.LOCKFREE);
        LOG.info("Starting batch producer-side JLBH with queueType={} queueSize={} batchSize={}",
                queueType, QUEUE_SIZE, BATCH_SIZE);

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS).iterations(ITERATIONS)
                .throughput(THROUGHPUT).runs(RUNS).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHSPSCBatchProducerCircularQueue(queueType));

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        queue = queueType.create(QUEUE_SIZE);
        SPSCQueueConsumer consumer = new SPSCQueueConsumer(queue, true);
        consumerThread = new Thread(consumer::run, "JLBH Consumer");
        consumerThread.start();
        Arrays.fill(batch, 0L);

    }

    @Override
    public void run(long startTimeNS) {
        batch[0] = startTimeNS;
        while (!queue.batchAdd(batch)) {
            Thread.onSpinWait();
        }
        jlbh.sampleNanos(System.nanoTime() - startTimeNS);
    }

    @Override
    public void complete() {
        consumerThread.interrupt();
    }
}

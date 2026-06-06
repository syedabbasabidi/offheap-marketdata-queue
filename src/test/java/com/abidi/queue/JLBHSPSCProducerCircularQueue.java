package com.abidi.queue;

import com.abidi.consumer.SPSCQueueConsumer;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JLBHSPSCProducerCircularQueue implements JLBHTask {

    public static final int ITERATIONS = 50_000_000;
    public static final int THROUGHPUT = 100_000_000;
    public static final int RUNS = 10;
    public static final int WARM_UP_ITERATIONS = 10_000;
    public static final int QUEUE_SIZE = 65536;
    private JLBH jlbh;
    private SPSCCircularQueue queue;
    private Thread consumerThread;
    private final JLBHSPSCQueueType queueType;

    private static final Logger LOG = LoggerFactory.getLogger(JLBHSPSCProducerCircularQueue.class);

    public JLBHSPSCProducerCircularQueue(JLBHSPSCQueueType queueType) {
        this.queueType = queueType;
    }

    public static void main(String[] args) {
        JLBHSPSCQueueType queueType = JLBHSPSCQueueType.fromArg(args, JLBHSPSCQueueType.LOCKFREE);
        LOG.info("Starting producer-side JLBH with queueType={} queueSize={}", queueType, QUEUE_SIZE);

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS).iterations(ITERATIONS)
                .throughput(THROUGHPUT).runs(RUNS).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHSPSCProducerCircularQueue(queueType));

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        queue = queueType.create(QUEUE_SIZE);
        SPSCQueueConsumer consumer = new SPSCQueueConsumer(queue);
        consumerThread = new Thread(consumer::run, "JLBH Consumer");
        consumerThread.start();
    }

    @Override
    public void run(long startTimeNS) {
        while (!queue.add(startTimeNS)) {
            Thread.onSpinWait();
        }
        jlbh.sampleNanos(System.nanoTime() - startTimeNS);
    }

    @Override
    public void complete() {
        consumerThread.interrupt();
    }
}

package com.abidi.queue;

import com.abidi.marketdata.model.MarketData;
import com.abidi.producer.SPSCQueueProducer;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JLBHSPSCConsumerCircularQueue implements JLBHTask {

    public static final int ITERATIONS = 50_000_000;
    public static final int THROUGHPUT = 10_000_000;
    public static final int RUNS = 5;
    public static final int WARM_UP_ITERATIONS = 10_000;
    public static final int QUEUE_SIZE = 65536;
    private JLBH jlbh;
    private SPSCCircularQueue<MarketData> queue;
    private Thread producerThread;
    private final JLBHSPSCQueueType queueType;

    private static final Logger LOG = LoggerFactory.getLogger(JLBHSPSCConsumerCircularQueue.class);

    public JLBHSPSCConsumerCircularQueue(JLBHSPSCQueueType queueType) {
        this.queueType = queueType;
    }

    public static void main(String[] args) {
        JLBHSPSCQueueType queueType = JLBHSPSCQueueType.fromArg(args, JLBHSPSCQueueType.LOCKFREE);
        LOG.info("Starting consumer-side JLBH with queueType={} queueSize={}", queueType, QUEUE_SIZE);

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS).iterations(ITERATIONS)
                .throughput(THROUGHPUT).runs(RUNS).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHSPSCConsumerCircularQueue(queueType));

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        queue = queueType.create(QUEUE_SIZE);
        SPSCQueueProducer producer = new SPSCQueueProducer(queue);
        producerThread = new Thread(producer::run, "JLBH Producer");
        producerThread.start();
    }

    @Override
    public void run(long startTimeNS) {
        MarketData msg = queue.get();
        while (msg == null) {
            Thread.onSpinWait();
            msg = queue.get();
        }
        jlbh.sampleNanos( System.nanoTime() - startTimeNS);
    }

    @Override
    public void complete() {
        producerThread.interrupt();
    }
}

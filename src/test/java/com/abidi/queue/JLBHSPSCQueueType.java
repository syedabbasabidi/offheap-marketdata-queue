package com.abidi.queue;

import com.abidi.marketdata.model.MarketData;

enum JLBHSPSCQueueType {
    VOLATILE {
        @Override
        SPSCCircularQueue<MarketData> create(int size) {
            return new SPSCVolatileCircularQueue(size);
        }
    },
    LOCKFREE {
        @Override
        SPSCCircularQueue<MarketData> create(int size) {
            return new SPSCLockFreeCircularQueue(size);
        }
    };

    static JLBHSPSCQueueType fromArg(String[] args, JLBHSPSCQueueType defaultType) {
        if (args.length == 0) {
            return defaultType;
        }

        String queueType = args[0].toLowerCase();
        switch (queueType) {
            case "volatile":
                return VOLATILE;
            case "lockfree":
                return LOCKFREE;
            default:
                throw new IllegalArgumentException(
                        "Unknown queue type '" + args[0] + "'. Expected one of: volatile, lockfree");
        }
    }

    abstract SPSCCircularQueue<MarketData> create(int size);
}

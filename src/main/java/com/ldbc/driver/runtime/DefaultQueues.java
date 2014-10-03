package com.ldbc.driver.runtime;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

public class DefaultQueues {
    public static <T> Queue<T> newNonBlocking() {
        return new ConcurrentLinkedQueue<>();
    }

    public static <T> BlockingQueue<T> newBlockingUnbounded() {
        return new LinkedTransferQueue<>();
    }

    public static final int DEFAULT_BOUND = 100;

    public static <T> BlockingQueue<T> newBlockingBounded(int capacity) {
        return new LinkedBlockingQueue<>(capacity);
    }
}

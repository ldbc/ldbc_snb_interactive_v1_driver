package com.ldbc.driver.runtime;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;

public abstract class QueueEventSubmitter<EVENT_TYPE> {

    public static <TYPE> QueueEventSubmitter<TYPE> queueEventSubmitterFor(Queue<TYPE> queue) {
        return (BlockingQueue.class.isAssignableFrom(queue.getClass()))
                ? new BlockingQueueEventSubmitter((BlockingQueue) queue)
                : new NonBlockingQueueEventSubmitter(queue);
    }

    public abstract void submitEventToQueue(EVENT_TYPE event) throws InterruptedException;

    static class NonBlockingQueueEventSubmitter<EVENT_TYPE_NON_BLOCKING> extends QueueEventSubmitter<EVENT_TYPE_NON_BLOCKING> {
        private final Queue<EVENT_TYPE_NON_BLOCKING> queue;

        private NonBlockingQueueEventSubmitter(Queue<EVENT_TYPE_NON_BLOCKING> queue) {
            this.queue = queue;
        }

        @Override
        public void submitEventToQueue(EVENT_TYPE_NON_BLOCKING event) throws InterruptedException {
            while (false == queue.offer(event)) {
                LockSupport.parkNanos(1);
            }
        }
    }

    static class BlockingQueueEventSubmitter<EVENT_TYPE_BLOCKING> extends QueueEventSubmitter<EVENT_TYPE_BLOCKING> {
        private final BlockingQueue<EVENT_TYPE_BLOCKING> queue;

        private BlockingQueueEventSubmitter(BlockingQueue<EVENT_TYPE_BLOCKING> queue) {
            this.queue = queue;
        }

        @Override
        public void submitEventToQueue(EVENT_TYPE_BLOCKING event) throws InterruptedException {
            queue.put(event);
        }
    }
}

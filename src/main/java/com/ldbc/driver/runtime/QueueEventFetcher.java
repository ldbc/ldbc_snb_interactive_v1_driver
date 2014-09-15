package com.ldbc.driver.runtime;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

public abstract class QueueEventFetcher<EVENT_TYPE> {
    public abstract EVENT_TYPE fetchNextEvent() throws InterruptedException;

    public static <TYPE> QueueEventFetcher<TYPE> queueEventFetcherFor(Queue<TYPE> queue) {
        return (BlockingQueue.class.isAssignableFrom(queue.getClass()))
                ? new BlockingQueueEventFetcher((BlockingQueue) queue)
                : new NonBlockingQueueEventFetcher(queue);
    }

    static class NonBlockingQueueEventFetcher<EVENT_TYPE_NON_BLOCKING> extends QueueEventFetcher<EVENT_TYPE_NON_BLOCKING> {
        private final Queue<EVENT_TYPE_NON_BLOCKING> queue;

        public NonBlockingQueueEventFetcher(Queue<EVENT_TYPE_NON_BLOCKING> queue) {
            this.queue = queue;
        }

        @Override
        public EVENT_TYPE_NON_BLOCKING fetchNextEvent() throws InterruptedException {
            EVENT_TYPE_NON_BLOCKING event = null;
            while (event == null) {
                event = queue.poll();
            }
            return event;
        }
    }

    static class BlockingQueueEventFetcher<EVENT_TYPE_BLOCKING> extends QueueEventFetcher<EVENT_TYPE_BLOCKING> {
        private final BlockingQueue<EVENT_TYPE_BLOCKING> queue;

        public BlockingQueueEventFetcher(BlockingQueue<EVENT_TYPE_BLOCKING> queue) {
            this.queue = queue;
        }

        @Override
        public EVENT_TYPE_BLOCKING fetchNextEvent() throws InterruptedException {
            return queue.take();
        }
    }
}

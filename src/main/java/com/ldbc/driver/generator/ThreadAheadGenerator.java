package com.ldbc.driver.generator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadAheadGenerator<GENERATE_TYPE> implements Iterator<GENERATE_TYPE> {
    private final ThreadAheadGeneratorThread threadAheadGeneratorThread;
    private final BlockingQueue<GENERATE_TYPE> queue;
    private long itemsConsumed;
    private GENERATE_TYPE next;

    public ThreadAheadGenerator(Iterator<GENERATE_TYPE> inner, int threadAheadDistance) {
        this.queue = new ArrayBlockingQueue<>(threadAheadDistance);
        this.itemsConsumed = 0;
        this.next = null;
        this.threadAheadGeneratorThread = new ThreadAheadGeneratorThread(inner, queue);
        this.threadAheadGeneratorThread.start();
    }

    @Override
    public boolean hasNext() {
        if (null == next)
            next = doNext();
        return null != next;
    }

    @Override
    public GENERATE_TYPE next() {
        if (null == next)
            next = doNext();
        if (null == next)
            throw new NoSuchElementException(getClass().getSimpleName() + " has nothing more to generate");
        GENERATE_TYPE tempNext = next;
        next = null;
        itemsConsumed++;
        return tempNext;
    }

    public void forceShutdown() {
        threadAheadGeneratorThread.forceShutdown();
    }

    int i = 0;

    private GENERATE_TYPE doNext() throws GeneratorException {
        try {
            if (itemsConsumed < threadAheadGeneratorThread.itemsProduced()) {
                // queue not empty --> return next thing
                return queue.take();

            } else if (threadAheadGeneratorThread.finished()) {
                if (null == threadAheadGeneratorThread.error() && itemsConsumed >= threadAheadGeneratorThread.itemsProduced()) {
                    // queue empty && thread finished && no error --> we're done
                    return null;
                } else {
                    // queue empty && thread finished && error --> throw exception
                    throw new GeneratorException("Encountered error while retrieving next thing from queue", threadAheadGeneratorThread.error());
                }

            } else {
                GENERATE_TYPE tempNext;
                // queue empty && thread not finished --> more might come, wait for next thing
                while (null == (tempNext = queue.poll())) {
                    if (threadAheadGeneratorThread.finished()) {
                        if (null != threadAheadGeneratorThread.error()) {
                            // queue empty && thread finished && error --> throw exception
                            throw new GeneratorException("Encountered error while retrieving next thing from queue", threadAheadGeneratorThread.error());
                        }
                        if (itemsConsumed >= threadAheadGeneratorThread.itemsProduced()) {
                            // queue empty && thread finished && no error --> we're done
                            return null;
                        }
                    }
                }
                // new thing arrived --> return next thing
                return tempNext;
            }
        } catch (Throwable e) {
            throw new GeneratorException("Encountered error while retrieving next thing from queue", e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported by " + getClass().getSimpleName());
    }

    private class ThreadAheadGeneratorThread extends Thread {
        private final Iterator<GENERATE_TYPE> inner;
        private final BlockingQueue<GENERATE_TYPE> queue;
        private final AtomicLong itemsProduced;
        private final AtomicBoolean finished;
        private final AtomicBoolean forcedShutdown;
        private AtomicReference<Throwable> error;

        private ThreadAheadGeneratorThread(Iterator<GENERATE_TYPE> inner, BlockingQueue<GENERATE_TYPE> queue) {
            this.inner = inner;
            this.queue = queue;
            this.itemsProduced = new AtomicLong(0);
            this.finished = new AtomicBoolean(false);
            this.forcedShutdown = new AtomicBoolean(false);
            this.error = new AtomicReference<>(null);
        }

        private void forceShutdown() {
            finished.set(true);
            itemsProduced.set(-1);
            forcedShutdown.set(true);
        }

        private boolean finished() {
            return finished.get();
        }

        private long itemsProduced() {
            return itemsProduced.get();
        }

        private Throwable error() {
            return error.get();
        }


        @Override
        public void run() {
            try {
                while (inner.hasNext() && false == forcedShutdown.get()) {
                    GENERATE_TYPE next = inner.next();
                    itemsProduced.incrementAndGet();
                    queue.put(next);
                }
                finished.set(true);
            } catch (Throwable e) {
                error.set(e);
                itemsProduced.set(-1);
                finished.set(true);
            }
        }
    }
}

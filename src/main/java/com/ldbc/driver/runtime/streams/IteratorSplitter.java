package com.ldbc.driver.runtime.streams;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Splits an Iterator into multiple Iterables. This operation is NOT lazy, all items from the input Iterator are materialized in the output Iterables.
 */
public class IteratorSplitter<ITEMS_TYPE> {
    private static final Duration SPIN_SLEEP_DURATION = Duration.fromMilli(100);

    public enum UnmappedItemPolicy {
        // If an item is found that does not have an associated split it is ignored and the next is retrieved
        DROP,
        // If an item is found that does not have an associated split the splitting process is aborted
        ABORT
    }

    private final UnmappedItemPolicy unmappedItemPolicy;

    public IteratorSplitter(UnmappedItemPolicy unmappedItemPolicy) {
        this.unmappedItemPolicy = unmappedItemPolicy;
    }

    /**
     * Registers split definitions with splits, then populates those splits and returns them a split result
     *
     * @param inputIterator original, un-split iterator
     * @param definitions   item types to associate with splits
     * @return a SplitResult containing all registered splits
     * @throws IteratorSplittingException if an item type is registered with more than one split
     */
    public SplitResult<ITEMS_TYPE> split(Iterator<? extends ITEMS_TYPE> inputIterator, SplitDefinition<ITEMS_TYPE>... definitions) throws IteratorSplittingException {
        AtomicBoolean inputStreamIsExhausted = new AtomicBoolean(false);
        AtomicInteger splitListCreateThreadCount = new AtomicInteger(0);
        Map<Class<? extends ITEMS_TYPE>, Queue<ITEMS_TYPE>> outputQueueMapping = new HashMap<>();
        List<ItemIteratorToItemListThread> splitListCreateThreads = new ArrayList<>();
        SplitResult<ITEMS_TYPE> splitResult = new SplitResult<>();
        for (SplitDefinition<ITEMS_TYPE> definition : definitions) {
            Queue<ITEMS_TYPE> splitQueue = new ConcurrentLinkedQueue<>();
            for (Class<? extends ITEMS_TYPE> itemType : definition.itemTypes()) {
                outputQueueMapping.put(itemType, splitQueue);
            }
            List<ITEMS_TYPE> splitList = splitResult.addSplit(definition);
            Iterator<ITEMS_TYPE> splitIterator = new ItemQueueReadingIterator(splitQueue, inputStreamIsExhausted);
            ItemIteratorToItemListThread itemIteratorToItemListThread = new ItemIteratorToItemListThread(splitIterator, splitList, splitListCreateThreadCount);
            splitListCreateThreads.add(itemIteratorToItemListThread);
        }

        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ItemQueueSplittingThread splittingThread =
                new ItemQueueSplittingThread(inputIterator, inputStreamIsExhausted, outputQueueMapping, unmappedItemPolicy, errorReporter);
        splittingThread.start();

        splitListCreateThreadCount.set(splitListCreateThreads.size());
        for (ItemIteratorToItemListThread splitListCreateThread : splitListCreateThreads) {
            splitListCreateThread.start();
        }
        while (splitListCreateThreadCount.get() > 0) {
            if (errorReporter.errorEncountered()) {
                splittingThread.interrupt();
                for (ItemIteratorToItemListThread splitListCreateThread : splitListCreateThreads) {
                    splitListCreateThread.interrupt();
                }
                throw new IteratorSplittingException(String.format("Splitting terminated\n%s", errorReporter.toString()));
            }
            sleep(SPIN_SLEEP_DURATION);
        }
        if (errorReporter.errorEncountered()) {
            throw new IteratorSplittingException(String.format("Splitting terminated\n%s", errorReporter.toString()));
        }
        return splitResult;
    }

    private void sleep(Duration sleepDuration) {
        try {
            Thread.sleep(sleepDuration.asMilli());
        } catch (InterruptedException e) {
        }
    }

    private class ItemQueueReadingIterator implements Iterator<ITEMS_TYPE> {
        private final Queue<ITEMS_TYPE> inputQueue;
        private final AtomicBoolean inputStreamIsExhausted;
        private ITEMS_TYPE next = null;

        private ItemQueueReadingIterator(Queue<ITEMS_TYPE> inputQueue, AtomicBoolean inputStreamIsExhausted) {
            this.inputQueue = inputQueue;
            this.inputStreamIsExhausted = inputStreamIsExhausted;
        }

        @Override
        public boolean hasNext() {
            next = (next == null) ? doNext() : next;
            return (next != null);
        }

        @Override
        public ITEMS_TYPE next() {
            next = (next == null) ? doNext() : next;
            if (null == next) throw new NoSuchElementException("No items remain");
            ITEMS_TYPE tempNext = next;
            next = null;
            return tempNext;
        }

        private ITEMS_TYPE doNext() {
            ITEMS_TYPE nextItem = null;
            while (false == inputQueue.isEmpty() || false == inputStreamIsExhausted.get()) {
                nextItem = inputQueue.poll();
                if (null != nextItem) break;
            }
            return nextItem;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException(String.format("%s does not support remove()", getClass().getSimpleName()));
        }
    }

    private class ItemIteratorToItemListThread extends Thread {
        private final Iterator<ITEMS_TYPE> inputItemIterator;
        private final List<ITEMS_TYPE> outputItemList;
        private final AtomicInteger splitListCreateThreadCount;

        private ItemIteratorToItemListThread(Iterator<ITEMS_TYPE> inputItemIterator,
                                             List<ITEMS_TYPE> outputItemList,
                                             AtomicInteger splitListCreateThreadCount) {
            super(ItemIteratorToItemListThread.class.getSimpleName() + "-" + System.currentTimeMillis());
            this.inputItemIterator = inputItemIterator;
            this.outputItemList = outputItemList;
            this.splitListCreateThreadCount = splitListCreateThreadCount;
        }

        @Override
        public void run() {
            while (inputItemIterator.hasNext()) {
                outputItemList.add(inputItemIterator.next());
            }
            splitListCreateThreadCount.decrementAndGet();
        }
    }

    private class ItemQueueSplittingThread extends Thread {
        private final Iterator<? extends ITEMS_TYPE> inputItemIterator;
        private final AtomicBoolean inputStreamIsExhausted;
        private final Map<Class<? extends ITEMS_TYPE>, Queue<ITEMS_TYPE>> outputQueueMapping;
        private final UnmappedItemPolicy unmappedItemPolicy;
        private final ConcurrentErrorReporter errorReporter;

        private ItemQueueSplittingThread(Iterator<? extends ITEMS_TYPE> inputItemIterator,
                                         AtomicBoolean inputStreamIsExhausted,
                                         Map<Class<? extends ITEMS_TYPE>, Queue<ITEMS_TYPE>> outputQueueMapping,
                                         UnmappedItemPolicy unmappedItemPolicy,
                                         ConcurrentErrorReporter errorReporter) {
            super(ItemQueueSplittingThread.class.getSimpleName() + "-" + System.currentTimeMillis());
            this.inputItemIterator = inputItemIterator;
            this.inputStreamIsExhausted = inputStreamIsExhausted;
            this.outputQueueMapping = outputQueueMapping;
            this.unmappedItemPolicy = unmappedItemPolicy;
            this.errorReporter = errorReporter;
        }

        @Override
        public void run() {
            while (inputItemIterator.hasNext()) {
                ITEMS_TYPE nextItem = inputItemIterator.next();
                Queue<ITEMS_TYPE> outputItemQueue = outputQueueMapping.get(nextItem.getClass());
                if (null == outputItemQueue) {
                    switch (unmappedItemPolicy) {
                        case DROP:
                            continue;
                        case ABORT:
                            errorReporter.reportError(
                                    this,
                                    String.format("Item type has no associated split mapping: %s", nextItem.getClass().getSimpleName()));
                            break;
                        default:
                            errorReporter.reportError(
                                    this,
                                    String.format("Unknown UnmappedItemPolicy: %s", unmappedItemPolicy.name())
                            );
                            break;
                    }
                } else {
                    try {
                        outputItemQueue.add(nextItem);
                    } catch (Exception e) {
                        errorReporter.reportError(
                                this,
                                String.format("Unable to add item to output queue\n%s", ConcurrentErrorReporter.stackTraceToString(e))
                        );
                    }
                }
            }
            inputStreamIsExhausted.set(true);
        }
    }
}
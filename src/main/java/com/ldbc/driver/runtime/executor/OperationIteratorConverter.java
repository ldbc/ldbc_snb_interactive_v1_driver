package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

// TODO test
// TODO is this the way we want to do this? it will create yet another thread, leaving less for workload generation
// TODO excluding worker threads we already have threads for AT LEAST: main, status, metrics, gct, coordination (external gct, commands, etc)
public class OperationIteratorConverter extends Thread {
    private Map<OperationClassification, BlockingQueue<Operation<?>>> streams;

    private Iterator<Operation<?>> stream;
    private Map<Class<?>, OperationClassification> mapping;

    public OperationIteratorConverter(Iterator<Operation<?>> stream, Map<Class<?>, OperationClassification> mapping) {
        this.stream = stream;
        this.mapping = mapping;

        streams = new HashMap<OperationClassification, BlockingQueue<Operation<?>>>();
        streams.put(OperationClassification.WindowFalse_GCTRead, new LinkedBlockingDeque<Operation<?>>());
        streams.put(OperationClassification.WindowFalse_GCTReadWrite, new LinkedBlockingDeque<Operation<?>>());
        streams.put(OperationClassification.WindowTrue_GCTRead, new LinkedBlockingDeque<Operation<?>>());
        streams.put(OperationClassification.WindowTrue_GCTReadWrite, new LinkedBlockingDeque<Operation<?>>());
    }

    public void run() {
        while (stream.hasNext()) {
            Operation<?> op = stream.next();
            OperationClassification opClassification = mapping.get(op.getClass());
            BlockingQueue<Operation<?>> queue = streams.get(opClassification);
            try {
                queue.put(op);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Iterator<Operation<?>> getIterator(OperationClassification c) {
        return streams.get(c).iterator();
    }
}

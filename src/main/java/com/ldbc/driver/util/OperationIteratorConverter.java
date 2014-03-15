package com.ldbc.driver.util;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.WorkloadException;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

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

package com.ldbc.driver.workloads.scaling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.driver.OperationClassification.GctMode;
import static com.ldbc.driver.OperationClassification.SchedulingMode;

public class ScaleTestWorkload extends Workload {
    @Override
    public Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications() {
        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<Class<? extends Operation<?>>, OperationClassification>();
        /*
         * Modes (with examples from LDBC Interactive SNB Workload):
         * - WINDOWED & NONE ------------------->   n/a
         * - WINDOWED & READ ------------------->   Add Friendship
         * - WINDOWED & READ WRITE ------------->   Add Person
         * - INDIVIDUAL_BLOCKING & NONE -------->   n/a
         * - INDIVIDUAL_BLOCKING & READ -------->   Add Post
         *                                          Add Comment
         *                                          Add Post Like
         *                                          Add Comment Like
         *                                          Add Forum
         *                                          Add Forum Membership
         * - INDIVIDUAL_BLOCKING & READ WRITE -->   n/a
         * - INDIVIDUAL_ASYNC & NONE ----------->   Reads 1-14
         * - INDIVIDUAL_ASYNC & READ ----------->   n/a
         * - INDIVIDUAL_ASYNC & READ WRITE ----->   n/a
        */
        operationClassifications.put(WindowedReadOperation.class, new OperationClassification(SchedulingMode.WINDOWED, GctMode.READ));
        operationClassifications.put(WindowedReadWriteOperation.class, new OperationClassification(SchedulingMode.WINDOWED, GctMode.READ_WRITE));
        operationClassifications.put(SequentialReadOperation.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ));
        return operationClassifications;
    }

    @Override
    public void onInit(Map<String, String> properties) throws WorkloadException {
    }

    @Override
    protected void onCleanup() throws WorkloadException {
    }

    @Override
    protected Iterator<Operation<?>> createOperations(GeneratorFactory generators) throws WorkloadException {
        Iterator<Operation<?>> windowedReadOperations = new WindowedReadOperationIterator();
        Iterator<Operation<?>> windowedReadWriteOperations = new WindowedReadWriteOperationIterator();
        Iterator<Operation<?>> sequentialReadOperations = new SequentialReadOperationIterator();
         return null;
    }

    @Override
    protected Iterator<Tuple.Tuple2<Operation<?>, Object>> validationOperations(GeneratorFactory generators) throws WorkloadException {
        return null;
    }

    public static class WindowedReadOperationIterator implements Iterator<Operation<?>> {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public WindowedReadOperation next() {
            return new WindowedReadOperation();
        }

        @Override
        public void remove() {
        }
    }

    public static class WindowedReadWriteOperationIterator implements Iterator<Operation<?>> {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public WindowedReadWriteOperation next() {
            return new WindowedReadWriteOperation();
        }

        @Override
        public void remove() {
        }
    }

    public static class SequentialReadOperationIterator implements Iterator<Operation<?>> {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public SequentialReadOperation next() {
            return new SequentialReadOperation();
        }

        @Override
        public void remove() {
        }
    }

    public static class WindowedReadWriteOperation extends Operation<Object> {
    }

    public static class WindowedReadOperation extends Operation<Object> {
    }

    public static class SequentialReadOperation extends Operation<Object> {
    }
}

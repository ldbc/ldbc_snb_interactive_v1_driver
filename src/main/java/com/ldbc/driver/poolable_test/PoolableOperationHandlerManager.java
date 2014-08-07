package com.ldbc.driver.poolable_test;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.ClassLoaderHelper;
import stormpot.Allocator;
import stormpot.Poolable;
import stormpot.Slot;

import java.util.concurrent.Callable;

public class PoolableOperationHandlerManager {

    public static class PoolableOperationHandlerAllocator implements Allocator<PoolableOperationHandler<?>> {
        private final Class<? extends PoolableOperationHandler> poolableOperationHandlerClass;

        public PoolableOperationHandlerAllocator(Class<? extends PoolableOperationHandler> poolableOperationHandlerClass) {
            // TODO remove
            System.out.println(PoolableOperationHandlerAllocator.class + "()");
            this.poolableOperationHandlerClass = poolableOperationHandlerClass;
        }

        @Override
        public PoolableOperationHandler<?> allocate(Slot slot) throws Exception {
            // TODO remove
            System.out.println(PoolableOperationHandlerAllocator.class + ".allocate()");
            PoolableOperationHandler<?> poolableOperationHandler = ClassLoaderHelper.loadPoolableOperationHandler(poolableOperationHandlerClass);
            poolableOperationHandler.setSlot(slot);
            return poolableOperationHandler;
        }

        // TODO remove
        // THIS NEVER SEEMS TO GET CALLED!
        @Override
        public void deallocate(PoolableOperationHandler poolableOperationHandler) throws Exception {
            // TODO remove
            System.out.println(PoolableOperationHandlerAllocator.class + ".deallocate()");
            poolableOperationHandler.unInitialize();
        }
    }

    public static abstract class PoolableOperationHandler<OPERATION_TYPE extends Operation<?>> implements Callable<OperationResultReport>, Poolable {
        private Slot slot;
        private TimeSource TIME_SOURCE;
        private Spinner spinner;
        private OPERATION_TYPE operation;
        private LocalCompletionTimeWriter localCompletionTimeWriter;
        private ConcurrentErrorReporter errorReporter;
        private ConcurrentMetricsService metricsService;

        private boolean initialized = false;

        public void setSlot(Slot slot) {
            // TODO remove
            System.out.println(PoolableOperationHandler.class + ".setSlot()");
            this.slot = slot;
        }

        public void init(TimeSource timeSource,
                         Spinner spinner,
                         Operation<?> operation,
                         LocalCompletionTimeWriter localCompletionTimeWriter,
                         ConcurrentErrorReporter errorReporter,
                         ConcurrentMetricsService metricsService) throws OperationException {
            // TODO remove
            System.out.println(PoolableOperationHandler.class + ".init()");
            if (initialized) {
                throw new OperationException(String.format("OperationHandler can not be initialized twice\n%s", toString()));
            }

            this.TIME_SOURCE = timeSource;
            this.spinner = spinner;
            this.operation = (OPERATION_TYPE) operation;
            this.localCompletionTimeWriter = localCompletionTimeWriter;
            this.errorReporter = errorReporter;
            this.metricsService = metricsService;

            initialized = true;
        }

        public final OPERATION_TYPE operation() {
            return operation;
        }

        public final LocalCompletionTimeWriter localCompletionTimeWriter() {
            return localCompletionTimeWriter;
        }

        @Override
        public OperationResultReport call() {
            // do nothing
            return null;
        }

        public final OperationResultReport executeOperationUnsafe(OPERATION_TYPE operation) throws DbException {
            // TODO remove
            System.out.println(PoolableOperationHandler.class + ".executeOperationUnsafe()");
            return executeOperation(operation);
        }

        protected abstract OperationResultReport executeOperation(OPERATION_TYPE operation) throws DbException;

        private void unInitialize() {
            // TODO remove
            System.out.println(PoolableOperationHandler.class + ".unInitialize()");
            initialized = false;
        }

        @Override
        public void release() {
            // TODO remove
            System.out.println(PoolableOperationHandler.class + ".release()");
            // THIS IS ONLY HERE BECAUSE PoolableOperationHandlerAllocator.deallocate() DOES NOT SEEM TO GET CALLED
            unInitialize();
            slot.release(this);
        }
    }
}
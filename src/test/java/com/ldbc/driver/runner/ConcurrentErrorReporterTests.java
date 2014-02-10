package com.ldbc.driver.runner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.ldbc.driver.util.Tuple;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConcurrentErrorReporterTests {
    @Test
    public void shouldMaintainOrderWhenErrorReportsAreSequential() throws InterruptedException, ExecutionException {
        // Given
        ConcurrentErrorReporter concurrentErrorReporter = new ConcurrentErrorReporter();

        // When
        concurrentErrorReporter.reportError(this, "Error 1");
        concurrentErrorReporter.reportError(this, "Error 2");
        concurrentErrorReporter.reportError(this, "Error 3");

        // Then
//        System.out.println(concurrentErrorReporter.toString());
        assertThat(concurrentErrorReporter.errorMessages().poll()._2(), is("Error 1"));
        assertThat(concurrentErrorReporter.errorMessages().poll()._2(), is("Error 2"));
        assertThat(concurrentErrorReporter.errorMessages().poll()._2(), is("Error 3"));
        assertThat(concurrentErrorReporter.errorEncountered(), is(true));
    }

    @Test
    public void shouldReportAllWhenErrorReportsAreConcurrent() throws InterruptedException, ExecutionException {
        // Given
        int taskCount = 100;
        int threadCount = 10;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService threadPoolExecutorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(threadPoolExecutorService);
        ConcurrentErrorReporter concurrentErrorReporter = new ConcurrentErrorReporter();

        // When
        for (int i = 0; i < taskCount; i++) {
            completionService.submit(new ErrorReportingTask(concurrentErrorReporter, i));
        }

        threadPoolExecutorService.shutdown();
        threadPoolExecutorService.awaitTermination(10, TimeUnit.SECONDS);

        // Then
//        System.out.println(concurrentErrorReporter.toString());
        assertThat(concurrentErrorReporter.errorMessages().size(), is(taskCount));
        assertThat(concurrentErrorReporter.errorEncountered(), is(true));
        List<String> errorMessages =
                ImmutableList.copyOf(
                        Iterables.transform(concurrentErrorReporter.errorMessages(), new Function<Tuple.Tuple2<String, String>, String>() {
                            @Override
                            public String apply(Tuple.Tuple2<String, String> input) {
                                return input._2();
                            }
                        }));
        for (int i = 0; i < taskCount; i++) {
            assertThat(errorMessages.contains(String.format("MyID=%s", i)), is(true));
        }
    }

    class ErrorReportingTask implements Callable<Integer> {
        private final ConcurrentErrorReporter concurrentErrorReporter;
        private final int myId;

        public ErrorReportingTask(ConcurrentErrorReporter concurrentErrorReporter, int id) {
            this.concurrentErrorReporter = concurrentErrorReporter;
            this.myId = id;
        }

        public Integer call() throws Exception {
            concurrentErrorReporter.reportError(this, String.format("MyID=%s", myId));
            return 0;
        }
    }

}
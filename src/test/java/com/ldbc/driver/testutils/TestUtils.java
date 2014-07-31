package com.ldbc.driver.testutils;

import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestUtils {
    public static File getResource(String path) {
        return FileUtils.toFile(TestUtils.class.getResource(path));
    }

    public static class ThreadPoolLoadGenerator {
        private final static Duration SHUTDOWN_WAIT = Duration.fromSeconds(5);
        private final int threadCount;
        private final ExecutorService executorService;
        private final Duration sleepDuration;
        private final AtomicBoolean sharedDoTerminateReference = new AtomicBoolean(false);

        public ThreadPoolLoadGenerator(int threadCount, Duration sleepDuration) {
            this.threadCount = threadCount;
            this.sleepDuration = sleepDuration;
            ThreadFactory threadFactory = new ThreadFactory() {
                private final long factoryTimeStampId = System.currentTimeMillis();
                int count = 0;

                @Override
                public Thread newThread(Runnable runnable) {
                    Thread newThread = new Thread(
                            runnable,
                            ThreadPoolLoadGenerator.class.getSimpleName() + "-id(" + factoryTimeStampId + ")" + "-thread(" + count++ + ")");
                    return newThread;
                }
            };

            this.executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        }

        public void start() {
            for (int i = 0; i < threadCount; i++) {
                LoadGeneratingTask task = new LoadGeneratingTask(sharedDoTerminateReference, sleepDuration);
                executorService.execute(task);
            }
        }

        synchronized public boolean shutdown() throws InterruptedException {
            if (true == sharedDoTerminateReference.get())
                return true;

            sharedDoTerminateReference.set(true);

            executorService.shutdown();
            boolean allHandlersCompleted = executorService.awaitTermination(SHUTDOWN_WAIT.asMilli(), TimeUnit.MILLISECONDS);
            return allHandlersCompleted;
        }

        private static class LoadGeneratingTask implements Runnable {
            private final AtomicBoolean sharedDoTerminateReference;
            private final long sleepDurationAsMilli;

            private LoadGeneratingTask(AtomicBoolean sharedDoTerminateReference, Duration sleepDuration) {
                this.sharedDoTerminateReference = sharedDoTerminateReference;
                this.sleepDurationAsMilli = sleepDuration.asMilli();
            }

            @Override
            public void run() {
                while (false == sharedDoTerminateReference.get()) {
                    Spinner.powerNap(sleepDurationAsMilli);
                }
            }
        }
    }
}

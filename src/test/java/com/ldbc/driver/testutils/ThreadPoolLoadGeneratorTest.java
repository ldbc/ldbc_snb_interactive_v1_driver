package com.ldbc.driver.testutils;

import com.ldbc.driver.runtime.scheduling.Spinner;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public class ThreadPoolLoadGeneratorTest {
    @Test
    public void shouldStartAndShutdownWithThreadCount1() throws InterruptedException {
        shouldStartAndShutdownWithThreadCount(1);
    }

    @Test
    public void shouldStartAndShutdownWithThreadCount10() throws InterruptedException {
        shouldStartAndShutdownWithThreadCount(10);
    }

    @Test
    public void shouldStartAndShutdownWithThreadCount100() throws InterruptedException {
        shouldStartAndShutdownWithThreadCount(100);
    }

    public void shouldStartAndShutdownWithThreadCount(int threadCount) throws InterruptedException {
        // Given
        long sleepDuration = 0l;
        long shutdownTimeout = 5000l;
        ThreadPoolLoadGenerator threadPoolLoadGenerator = new ThreadPoolLoadGenerator(threadCount, sleepDuration);

        // When
        threadPoolLoadGenerator.start();
        Spinner.powerNap(1000l);
        boolean shutdownSuccessfully = threadPoolLoadGenerator.shutdown(shutdownTimeout);

        // Then
        assertThat(shutdownSuccessfully, is(true));
    }
}

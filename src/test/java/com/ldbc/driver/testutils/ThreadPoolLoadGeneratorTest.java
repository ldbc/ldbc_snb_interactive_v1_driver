package com.ldbc.driver.testutils;

import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
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
        Duration sleepDuration = Duration.fromMilli(0);
        Duration shutdownTimeout = Duration.fromSeconds(5);
        ThreadPoolLoadGenerator threadPoolLoadGenerator = new ThreadPoolLoadGenerator(threadCount, sleepDuration);

        // When
        threadPoolLoadGenerator.start();
        Spinner.powerNap(Duration.fromSeconds(1).asMilli());
        boolean shutdownSuccessfully = threadPoolLoadGenerator.shutdown(shutdownTimeout);

        // Then
        assertThat(shutdownSuccessfully, is(true));
    }
}

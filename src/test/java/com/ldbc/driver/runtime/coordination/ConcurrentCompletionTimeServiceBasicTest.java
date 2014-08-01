package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class ConcurrentCompletionTimeServiceBasicTest {
    @Test
    public void shouldBehavePredictablyAfterInstantiationWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            shouldBehavePredictablyAfterInstantiation(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldBehavePredictablyAfterInstantiationWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            shouldBehavePredictablyAfterInstantiation(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void shouldBehavePredictablyAfterInstantiation(ConcurrentCompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        // instantiated completion time service

        // When
        // nothing

        // Then
        assertThat(completionTimeService.globalCompletionTime(), is(nullValue()));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    public void shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimesWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimes(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimesWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimes(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimes(ConcurrentCompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter writer1 = completionTimeService.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = completionTimeService.newLocalCompletionTimeWriter();

        // When
        writer1.submitLocalInitiatedTime(Time.fromMilli(0));
        writer1.submitLocalCompletedTime(Time.fromMilli(0));
        writer2.submitLocalInitiatedTime(Time.fromMilli(0));
        writer2.submitLocalCompletedTime(Time.fromMilli(0));

        writer1.submitLocalInitiatedTime(Time.fromMilli(1));
        writer2.submitLocalInitiatedTime(Time.fromMilli(1));

        // Then
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromMilli(0)));
        assertThat(completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
    }

    @Test
    public void shouldReturnAllWritersWithSynchronizedImplementation() throws CompletionTimeException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);


        // Then
        try {
            shouldReturnAllWriters(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnAllWritersWithThreadedImplementation() throws CompletionTimeException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);


        // Then
        try {
            shouldReturnAllWriters(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void shouldReturnAllWriters(ConcurrentCompletionTimeService completionTimeService) throws CompletionTimeException {
        // Given
        // instantiated completion time service

        // When/Then
        assertThat(completionTimeService.getAllWriters().size(), is(0));

        LocalCompletionTimeWriter writer1 = completionTimeService.newLocalCompletionTimeWriter();

        assertThat(completionTimeService.getAllWriters().size(), is(1));
        assertThat(completionTimeService.getAllWriters().contains(writer1), is(true));

        LocalCompletionTimeWriter writer2 = completionTimeService.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer3 = completionTimeService.newLocalCompletionTimeWriter();

        assertThat(completionTimeService.getAllWriters().size(), is(3));
        assertThat(completionTimeService.getAllWriters().contains(writer1), is(true));
        assertThat(completionTimeService.getAllWriters().contains(writer2), is(true));
        assertThat(completionTimeService.getAllWriters().contains(writer3), is(true));
    }
}

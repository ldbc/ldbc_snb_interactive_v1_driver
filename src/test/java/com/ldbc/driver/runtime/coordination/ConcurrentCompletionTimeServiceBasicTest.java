package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
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

    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTNoExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTNoExternalCTWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    // LocalIT = none, LocalCT = none, ExternalCT = none --> null
    public void doShouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT(ConcurrentCompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When
        // no events have been initiated or completed

        // Then
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenSomeITAndNoCTAndNoExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnNullWhenSomeITAndNoCTAndNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenSomeITAndNoCTAndNoExternalCTWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenSomeITAndNoCTAndNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    // LocalIT = some, LocalCT = none, ExternalCT = none --> null
    public void doShouldReturnNullWhenSomeITAndNoCTAndNoExternalCT(ConcurrentCompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(1));

        // Then
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCTWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = none, LocalCT = none, ExternalCT = some --> null
    public void doShouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCT(ConcurrentCompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        // Then
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCTWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = some, LocalCT = some, ExternalCT = none --> null
    public void doShouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCT(ConcurrentCompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(1));
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(1));

        // Then
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCTWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = 1, LocalCT = none, ExternalCT = 2 --> null
    public void doShouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCT(ConcurrentCompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(1));
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        // Then
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    public void shouldReturnGCTWhenLowerLCTThanExternalCTWithSynchronizedImplementation() throws CompletionTimeException, ExecutionException, InterruptedException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnLCTWhenLowerLCTThanExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnGCTWhenLowerLCTThanExternalCTWithThreadedImplementation() throws CompletionTimeException, ExecutionException, InterruptedException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnLCTWhenLowerLCTThanExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = 1, LocalCT = 1, ExternalCT = 2 --> 1
    public void doShouldReturnLCTWhenLowerLCTThanExternalCT(ConcurrentCompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, ExecutionException, InterruptedException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When/Then
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(1));
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(1));
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        assertThat(completionTimeService.globalCompletionTimeFuture().get(), is(nullValue()));

        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(2));

        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldReturnExternalCTWhenLowerLCTThanExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnExternalCTWhenLowerLCTThanExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnExternalCTWhenLowerLCTThanExternalCTWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnExternalCTWhenLowerLCTThanExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = 2, LocalCT = 2, ExternalCT =  --> 1
    public void doShouldReturnExternalCTWhenLowerLCTThanExternalCT(ConcurrentCompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When/Then
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(2));
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(2));
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(3));

        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeers(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeers(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeers(ConcurrentCompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When/Then
        // initiated [1]
        // completed []
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        // initiated [1,2]
        // completed [1]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));

        // initiated [1,2]
        // completed [1,2]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));

        // initiated [1,2,3]
        // completed [1,2]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4]
        // completed [1,2]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2, , ,5]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , , , ,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(6));
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(7));
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(8));
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(9));
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(10));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , ,8, ,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(8));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8, ,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(7));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8,9,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(9));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, ,4,5, ,7,8,9,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5, ,7,8,9,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(5)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(6));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(9)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,10]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(10));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(9)));

        // initiated [1,2,3,4,5,6,7,8,9,10,11]
        // completed [1,2,3,4,5,6,7,8,9,10,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(11));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(10)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimesWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimes(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimesWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimes(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimes(ConcurrentCompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When/Then
        // initiated [1]
        // completed [1]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        // initiated [1,2]
        // completed [1,2]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));

        // initiated [1,2,3]
        // completed [1,2, ]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3]
        // completed [1,2, , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3]
        // completed [1,2, , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4]
        // completed [1,2, , , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5]
        // completed [1,2, , , , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(6));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , ,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , ,3, ,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, ,3,3, ,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3, ,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(3)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(5)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5,6]
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(6));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(5)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimesWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        // Then
        try {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimesWithThreadedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        ConcurrentCompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes(completionTimeService, "other");
        } finally {
            try {
                completionTimeService.shutdown();
            } catch (Throwable e) {
                // do nothing, exception is expected because test was trying to force an error
            }
        }
    }

    public void doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes(ConcurrentCompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When/Then
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (1)
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(1));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (3)
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(nullValue()));

        // initiated [1,2]
        // completed [1]
        // external  (3)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));

        // initiated [1,2,3]
        // completed [1]
        // external  (3)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1]
        // external  (3)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        // external  (3)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        // external  (3)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (3)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (4)
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(4));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        // external  (4)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(4)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (4)
        localCompletionTimeWriter.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(4)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (5)
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(5));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(4)));

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        // external  (5)
        localCompletionTimeWriter.submitLocalInitiatedTime(Time.fromSeconds(6));
        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(5)));

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        // external  (4) <-- SHOULD NEVER DECREASE
        boolean exceptionThrown = false;
        try {
            externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(4));
            completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS);
        } catch (Throwable e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        assertThat(completionTimeService.globalCompletionTimeFuture().get(1, TimeUnit.SECONDS), is(Time.fromSeconds(5)));
    }
}

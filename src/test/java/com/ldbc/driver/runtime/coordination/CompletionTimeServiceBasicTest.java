package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CompletionTimeServiceBasicTest {
    @Test
    public void shouldBehavePredictablyAfterInstantiationWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            shouldBehavePredictablyAfterInstantiation(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void shouldBehavePredictablyAfterInstantiation(CompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        // instantiated completion time service

        // When
        // nothing

        // Then
        assertThat(completionTimeService.globalCompletionTimeAsMilli(), is(-1l));
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));
    }

    @Test
    public void shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimesWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimes(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void shouldAdvanceGctWhenWriterSubmitInitiatedAndCompletedTimes(CompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter writer1 = completionTimeService.newLocalCompletionTimeWriter();
        LocalCompletionTimeWriter writer2 = completionTimeService.newLocalCompletionTimeWriter();

        // When
        writer1.submitLocalInitiatedTime(0l);
        writer1.submitLocalCompletedTime(0l);
        writer2.submitLocalInitiatedTime(0l);
        writer2.submitLocalCompletedTime(0l);

        writer1.submitLocalInitiatedTime(1l);
        writer2.submitLocalInitiatedTime(1l);

        // Then
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(0l));
        assertThat(completionTimeService.globalCompletionTimeAsMilli(), is(0l));
    }

    @Test
    public void shouldReturnAllWritersWithSynchronizedImplementation() throws CompletionTimeException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);


        // Then
        try {
            shouldReturnAllWriters(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void shouldReturnAllWriters(CompletionTimeService completionTimeService) throws CompletionTimeException {
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
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    // LocalIT = none, LocalCT = none, ExternalCT = none --> null
    public void doShouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT(CompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When
        // no events have been initiated or completed

        // Then
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenSomeITAndNoCTAndNoExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = new HashSet<>();
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenSomeITAndNoCTAndNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    // LocalIT = some, LocalCT = none, ExternalCT = none --> null
    public void doShouldReturnNullWhenSomeITAndNoCTAndNoExternalCT(CompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime(1000l);

        // Then
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = none, LocalCT = none, ExternalCT = some --> null
    public void doShouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCT(CompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 1000l);

        // Then
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCT(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = some, LocalCT = some, ExternalCT = none --> null
    public void doShouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCT(CompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime(1000l);
        localCompletionTimeWriter.submitLocalCompletedTime(1000l);

        // Then
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = 1, LocalCT = none, ExternalCT = 2 --> null
    public void doShouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCT(CompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When
        localCompletionTimeWriter.submitLocalInitiatedTime(1000l);
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 2000l);

        // Then
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));
    }

    @Test
    public void shouldReturnGCTWhenLowerLCTThanExternalCTWithSynchronizedImplementation() throws CompletionTimeException, ExecutionException, InterruptedException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnLCTWhenLowerLCTThanExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = 1, LocalCT = 1, ExternalCT = 2 --> 1
    public void doShouldReturnLCTWhenLowerLCTThanExternalCT(CompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, ExecutionException, InterruptedException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When/Then
        localCompletionTimeWriter.submitLocalInitiatedTime(1000l);
        localCompletionTimeWriter.submitLocalCompletedTime(1000l);
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 2000l);

        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(), is(-1l));

        localCompletionTimeWriter.submitLocalInitiatedTime(2000l);

        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));
    }

    @Test
    public void shouldReturnExternalCTWhenLowerLCTThanExternalCTWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnExternalCTWhenLowerLCTThanExternalCT(completionTimeService, "other");
        } finally {
            completionTimeService.shutdown();
        }
    }

    //  LocalIT = 2, LocalCT = 2, ExternalCT =  --> 1
    public void doShouldReturnExternalCTWhenLowerLCTThanExternalCT(CompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When/Then
        localCompletionTimeWriter.submitLocalInitiatedTime(2000l);
        localCompletionTimeWriter.submitLocalCompletedTime(2000l);
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 1000l);

        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        localCompletionTimeWriter.submitLocalInitiatedTime(3000l);

        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeers(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeers(CompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When/Then
        // initiated [1]
        // completed []
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(1000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        // initiated [1]
        // completed [1]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(1000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        // initiated [1,2]
        // completed [1]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(2000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));

        // initiated [1,2]
        // completed [1,2]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(2000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));

        // initiated [1,2,3]
        // completed [1,2]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4]
        // completed [1,2]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(4000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(5000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2, , ,5]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(5000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , , , ,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(6000l);
        localCompletionTimeWriter.submitLocalInitiatedTime(7000l);
        localCompletionTimeWriter.submitLocalInitiatedTime(8000l);
        localCompletionTimeWriter.submitLocalInitiatedTime(9000l);
        localCompletionTimeWriter.submitLocalInitiatedTime(10000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , ,8, ,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(8000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8, ,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(7000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8,9,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(9000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, ,4,5, ,7,8,9,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(4000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5, ,7,8,9,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(5000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(6000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(9000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,10]
        // external  (-)
        localCompletionTimeWriter.submitLocalCompletedTime(10000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(9000l));

        // initiated [1,2,3,4,5,6,7,8,9,10,11]
        // completed [1,2,3,4,5,6,7,8,9,10,  ]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(11000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(10000l));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimesWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet();
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
                assistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);

        // Then
        try {
            doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimes(completionTimeService);
        } finally {
            completionTimeService.shutdown();
        }
    }

    public void doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimes(CompletionTimeService completionTimeService) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();

        // When/Then
        // initiated [1]
        // completed [1]
        localCompletionTimeWriter.submitLocalInitiatedTime(1000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));
        localCompletionTimeWriter.submitLocalCompletedTime(1000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        // initiated [1,2]
        // completed [1,2]
        localCompletionTimeWriter.submitLocalInitiatedTime(2000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));
        localCompletionTimeWriter.submitLocalCompletedTime(2000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));

        // initiated [1,2,3]
        // completed [1,2, ]
        localCompletionTimeWriter.submitLocalInitiatedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3]
        // completed [1,2, , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3,3]
        // completed [1,2, , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3,3,4]
        // completed [1,2, , , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(4000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3,3,4,5]
        // completed [1,2, , , , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(5000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , , , ]
        localCompletionTimeWriter.submitLocalInitiatedTime(6000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , ,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(5000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , ,3, ,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, ,3,3, ,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3, ,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(3000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5, ]
        localCompletionTimeWriter.submitLocalCompletedTime(4000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(5000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5,6]
        localCompletionTimeWriter.submitLocalCompletedTime(6000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(5000l));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimesWithSynchronizedImplementation() throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        Set<String> peerIds = Sets.newHashSet("other");
        CompletionTimeService completionTimeService =
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
        CompletionTimeService completionTimeService =
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

    public void doShouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes(CompletionTimeService completionTimeService, String otherPeerId) throws CompletionTimeException, InterruptedException, ExecutionException, TimeoutException {
        // Given
        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        // When/Then
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        // initiated [1]
        // completed [1]
        // external  (-)
        localCompletionTimeWriter.submitLocalInitiatedTime(1000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        localCompletionTimeWriter.submitLocalCompletedTime(1000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        // initiated [1]
        // completed [1]
        // external  (1)
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 1000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        // initiated [1]
        // completed [1]
        // external  (3)
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(-1l));

        // initiated [1,2]
        // completed [1]
        // external  (3)
        localCompletionTimeWriter.submitLocalInitiatedTime(2000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));

        // initiated [1,2,3]
        // completed [1]
        // external  (3)
        localCompletionTimeWriter.submitLocalInitiatedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));

        // initiated [1,2,3,4]
        // completed [1]
        // external  (3)
        localCompletionTimeWriter.submitLocalInitiatedTime(4000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        // external  (3)
        localCompletionTimeWriter.submitLocalCompletedTime(4000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(1000l));

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        // external  (3)
        localCompletionTimeWriter.submitLocalCompletedTime(2000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (3)
        localCompletionTimeWriter.submitLocalInitiatedTime(5000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (4)
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 4000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        // external  (4)
        localCompletionTimeWriter.submitLocalCompletedTime(3000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(4000l));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (4)
        localCompletionTimeWriter.submitLocalCompletedTime(5000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(4000l));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (5)
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 5000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(4000l));

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        // external  (5)
        localCompletionTimeWriter.submitLocalInitiatedTime(6000l);
        assertThat(completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS), is(5000l));

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        // external  (4) <-- SHOULD NEVER DECREASE
        boolean exceptionThrown = false;
        try {
            externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, 4000l);
            completionTimeService.globalCompletionTimeAsMilliFuture().get(1, TimeUnit.SECONDS);
        } catch (Throwable e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
    }
}

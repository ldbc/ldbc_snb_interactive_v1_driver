package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import com.ldbc.driver.temporal.Time;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

//Completion Time = min( min(Initiated Events), max(Completed Events) )
public class GlobalCompletionTimeTest {
    @Ignore
    @Test
    public void lookIntoMakingLocalCompletionTimeClassThatPerformsGlobalCompletionTimeFunctionalityOnSingleMachineBetweenDifferentThreadPools() {
        // TODO this is essential for correctness
        assertThat(true, is(false));
    }

    // LocalIT = none, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        // no events have been initiated or completed

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    // LocalIT = some, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenSomeITAndNoCTAndNoExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    // LocalIT = none, LocalCT = some, ExternalCT = none --> Exception
    @Test
    public void shouldThrowExceptionWhenNoLocalITAndSomeLocalCTAndNoExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        boolean exceptionThrown = false;
        try {
            gct.applyCompletedTime(Time.fromSeconds(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }

        // Then
        // exception should have already been thrown
        assertThat(exceptionThrown, is(true));
    }

    //  LocalIT = none, LocalCT = none, ExternalCT = some --> null
    @Test
    public void shouldReturnNullWhenNoLocalITAndNoLocalCTAndSomeExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    //  LocalIT = some, LocalCT = some, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(1));
        gct.applyCompletedTime(Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    //  LocalIT = 1, LocalCT = none, ExternalCT = 2 --> null
    @Test
    public void shouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(1));
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    //  LocalIT = 1, LocalCT = 1, ExternalCT = 2 --> 1
    @Test
    public void shouldReturnLCTWhenLowerLCTThanExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(1));
        gct.applyCompletedTime(Time.fromSeconds(1));
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        // Then
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 2, LocalCT = 2, ExternalCT =  --> 1
    @Test
    public void shouldReturnExternalCTWhenLowerLCTThanExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(2));
        gct.applyCompletedTime(Time.fromSeconds(2));
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeers() throws CompletionTimeException {
        // Given
        Set<String> noPeers = new HashSet<>();
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(noPeers);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When/Then
        gct.applyInitiatedTime(Time.fromSeconds(1));
        assertThat(gct.completionTime(), is(nullValue()));
        gct.applyCompletedTime(Time.fromSeconds(1));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));

        gct.applyInitiatedTime(Time.fromSeconds(2));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
        gct.applyCompletedTime(Time.fromSeconds(2));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        gct.applyInitiatedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        gct.applyInitiatedTime(Time.fromSeconds(4));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        gct.applyInitiatedTime(Time.fromSeconds(5));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));
        gct.applyCompletedTime(Time.fromSeconds(5));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimes() throws CompletionTimeException {
        // Given
        Set<String> noPeers = new HashSet<>();
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(noPeers);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When/Then
        // initiated [1]
        // completed [1]
        gct.applyInitiatedTime(Time.fromSeconds(1));
        assertThat(gct.completionTime(), is(nullValue()));
        gct.applyCompletedTime(Time.fromSeconds(1));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2]
        // completed [1,2]
        gct.applyInitiatedTime(Time.fromSeconds(2));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
        gct.applyCompletedTime(Time.fromSeconds(2));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3]
        // completed [1,2, ]
        gct.applyInitiatedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3]
        // completed [1,2, , ]
        gct.applyInitiatedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3]
        // completed [1,2, , , ]
        gct.applyInitiatedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4]
        // completed [1,2, , , , ]
        gct.applyInitiatedTime(Time.fromSeconds(4));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5]
        // completed [1,2, , , , , ]
        gct.applyInitiatedTime(Time.fromSeconds(5));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , , , ]
        gct.applyInitiatedTime(Time.fromSeconds(6));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , ,5, ]
        gct.applyCompletedTime(Time.fromSeconds(5));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , ,3, ,5, ]
        gct.applyCompletedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, ,3,3, ,5, ]
        gct.applyCompletedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3, ,5, ]
        gct.applyCompletedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(3)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5, ]
        gct.applyCompletedTime(Time.fromSeconds(4));
        assertThat(gct.completionTime(), is(Time.fromSeconds(5)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5,6]
        gct.applyCompletedTime(Time.fromSeconds(6));
        assertThat(gct.completionTime(), is(Time.fromSeconds(6)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When/Then
        // initiated [1]
        // completed [1]
        // external  (-)
        gct.applyInitiatedTime(Time.fromSeconds(1));
        assertThat(gct.completionTime(), is(nullValue()));
        gct.applyCompletedTime(Time.fromSeconds(1));
        assertThat(gct.completionTime(), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (1)
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(1));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));

        // initiated [1]
        // completed [1]
        // external  (3)
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2]
        // completed [1]
        // external  (3)
        gct.applyInitiatedTime(Time.fromSeconds(2));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3]
        // completed [1]
        // external  (3)
        gct.applyInitiatedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1]
        // external  (3)
        gct.applyInitiatedTime(Time.fromSeconds(4));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        // external  (3)
        gct.applyCompletedTime(Time.fromSeconds(4));
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        // external  (3)
        gct.applyCompletedTime(Time.fromSeconds(2));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (3)
        gct.applyInitiatedTime(Time.fromSeconds(5));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (4)
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(4));
        assertThat(gct.completionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        // external  (4)
        gct.applyCompletedTime(Time.fromSeconds(3));
        assertThat(gct.completionTime(), is(Time.fromSeconds(4)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (4)
        gct.applyCompletedTime(Time.fromSeconds(5));
        assertThat(gct.completionTime(), is(Time.fromSeconds(4)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (5)
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(5));
        assertThat(gct.completionTime(), is(Time.fromSeconds(5)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (4) <-- SHOULD NEVER DECREASE
        boolean exceptionThrown = false;
        try {
            gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(4));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(gct.completionTime(), is(Time.fromSeconds(5)));
    }
}
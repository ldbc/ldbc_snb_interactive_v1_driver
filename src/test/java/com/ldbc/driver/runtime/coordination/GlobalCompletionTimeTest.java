package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

//Completion Time = min( min(Initiated Events), max(Completed Events) )
public class GlobalCompletionTimeTest {

    // LocalIT = none, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When
        // no events have been initiated or completed

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));
    }

    // LocalIT = some, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenSomeITAndNoCTAndNoExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(1));

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));
    }

    // LocalIT = none, LocalCT = some, ExternalCT = none --> Exception
    @Test
    public void shouldThrowExceptionWhenNoLocalITAndSomeLocalCTAndNoExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When
        boolean exceptionThrown = false;
        try {
            globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(1));
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
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));
    }

    //  LocalIT = some, LocalCT = some, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenSomeLocalITAndSomeLocalCTAndNoExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(1));
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(1));

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));
    }

    //  LocalIT = 1, LocalCT = none, ExternalCT = 2 --> null
    @Test
    public void shouldReturnNullWhenSomeLocalITAndNoLocalCTAndSomeExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(1));
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));
    }

    //  LocalIT = 1, LocalCT = 1, ExternalCT = 2 --> 1
    @Test
    public void shouldReturnLCTWhenLowerLCTThanExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When/Then
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(1));
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(1));
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(2));

        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 2, LocalCT = 2, ExternalCT =  --> 1
    @Test
    public void shouldReturnExternalCTWhenLowerLCTThanExternalCT() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When/Then
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(2));
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(2));
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(3));

        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeers() throws CompletionTimeException {
        // Given
        Set<String> noPeers = new HashSet<>();
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(noPeers);
        ExternalCompletionTimeReader externalCompletionTimeReader =
                new LocalCompletionTimeReaderToExternalCompletionTimeReader(localCompletionTimeStateManager);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeReader,
                externalCompletionTimeStateManager);

        // When/Then
        // initiated [1]
        // completed []
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        // initiated [1,2]
        // completed [1]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2]
        // completed [1,2]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3]
        // completed [1,2]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4]
        // completed [1,2]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2, , ,5]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , , , ,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(6));
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(7));
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(8));
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(9));
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(10));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , ,8, ,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(8));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8, ,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(7));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8,9,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(9));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, ,4,5, ,7,8,9,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5, ,7,8,9,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(5)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(6));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(9)));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,10]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(10));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(9)));

        // initiated [1,2,3,4,5,6,7,8,9,10,11]
        // completed [1,2,3,4,5,6,7,8,9,10,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(11));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(10)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWhenNoPeersWithDuplicateTimes() throws CompletionTimeException {
        // Given
        Set<String> noPeers = new HashSet<>();
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(noPeers);
        ExternalCompletionTimeReader externalCompletionTimeReader =
                new LocalCompletionTimeReaderToExternalCompletionTimeReader(localCompletionTimeStateManager);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeReader,
                externalCompletionTimeStateManager);

        // When/Then
        // initiated [1]
        // completed [1]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        // initiated [1,2]
        // completed [1,2]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3]
        // completed [1,2, ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3]
        // completed [1,2, , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3]
        // completed [1,2, , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4]
        // completed [1,2, , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5]
        // completed [1,2, , , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(6));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , ,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, ,3,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(3)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(5)));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5,6]
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(6));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(5)));
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTWithDuplicateTimes() throws CompletionTimeException {
        // Given
        String otherPeerId = "otherPeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        ExternalCompletionTimeStateManager externalCompletionTimeStateManager = new ExternalCompletionTimeStateManager(peerIds);
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager,
                externalCompletionTimeStateManager,
                externalCompletionTimeStateManager);

        // When/Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(1));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(1));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (1)
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(1));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        // initiated [1]
        // completed [1]
        // external  (3)
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(nullValue()));

        // initiated [1,2]
        // completed [1]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(2));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3]
        // completed [1]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(4));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(4));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(1)));

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(2));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(5));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (4)
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(4));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(2)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        // external  (4)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(3));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(4)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (4)
        globalCompletionTimeStateManager.submitLocalCompletedTime(Time.fromSeconds(5));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(4)));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (5)
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(5));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(4)));

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        // external  (5)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(Time.fromSeconds(6));
        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(5)));

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        // external  (4) <-- SHOULD NEVER DECREASE
        boolean exceptionThrown = false;
        try {
            globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, Time.fromSeconds(4));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        assertThat(globalCompletionTimeStateManager.globalCompletionTime(), is(Time.fromSeconds(5)));
    }
}
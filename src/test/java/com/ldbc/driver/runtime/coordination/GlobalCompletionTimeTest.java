package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));
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
        globalCompletionTimeStateManager.submitLocalInitiatedTime(1000l);

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));
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
            globalCompletionTimeStateManager.submitLocalCompletedTime(1000l);
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
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 1000l);

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));
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
        globalCompletionTimeStateManager.submitLocalInitiatedTime(1000l);
        globalCompletionTimeStateManager.submitLocalCompletedTime(1000l);

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));
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
        globalCompletionTimeStateManager.submitLocalInitiatedTime(1000l);
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 2000l);

        // Then
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));
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
        globalCompletionTimeStateManager.submitLocalInitiatedTime(1000l);
        globalCompletionTimeStateManager.submitLocalCompletedTime(1000l);
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 2000l);

        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        globalCompletionTimeStateManager.submitLocalInitiatedTime(2000l);

        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));
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
        globalCompletionTimeStateManager.submitLocalInitiatedTime(2000l);
        globalCompletionTimeStateManager.submitLocalCompletedTime(2000l);
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 1000l);

        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        globalCompletionTimeStateManager.submitLocalInitiatedTime(3000l);

        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));
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
        globalCompletionTimeStateManager.submitLocalInitiatedTime(1000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        // initiated [1]
        // completed [1]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(1000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        // initiated [1,2]
        // completed [1]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(2000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));

        // initiated [1,2]
        // completed [1,2]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(2000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));

        // initiated [1,2,3]
        // completed [1,2]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4]
        // completed [1,2]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(4000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(5000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2, , ,5]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(5000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , , , ,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(6000l);
        globalCompletionTimeStateManager.submitLocalInitiatedTime(7000l);
        globalCompletionTimeStateManager.submitLocalInitiatedTime(8000l);
        globalCompletionTimeStateManager.submitLocalInitiatedTime(9000l);
        globalCompletionTimeStateManager.submitLocalInitiatedTime(10000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , ,8, ,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(8000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8, ,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(7000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8,9,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(9000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, ,4,5, ,7,8,9,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(4000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5, ,7,8,9,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(5000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(6000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(9000l));

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,10]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalCompletedTime(10000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(9000l));

        // initiated [1,2,3,4,5,6,7,8,9,10,11]
        // completed [1,2,3,4,5,6,7,8,9,10,  ]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(11000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(10000l));
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
        globalCompletionTimeStateManager.submitLocalInitiatedTime(1000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));
        globalCompletionTimeStateManager.submitLocalCompletedTime(1000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        // initiated [1,2]
        // completed [1,2]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(2000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));
        globalCompletionTimeStateManager.submitLocalCompletedTime(2000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));

        // initiated [1,2,3]
        // completed [1,2, ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3]
        // completed [1,2, , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3,3]
        // completed [1,2, , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3,3,4]
        // completed [1,2, , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(4000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3,3,4,5]
        // completed [1,2, , , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(5000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime(6000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(5000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , ,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, ,3,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(3000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime(4000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(5000l));

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5,6]
        globalCompletionTimeStateManager.submitLocalCompletedTime(6000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(5000l));
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
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        // initiated [1]
        // completed [1]
        // external  (-)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(1000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        globalCompletionTimeStateManager.submitLocalCompletedTime(1000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        // initiated [1]
        // completed [1]
        // external  (1)
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 1000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        // initiated [1]
        // completed [1]
        // external  (3)
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(-1l));

        // initiated [1,2]
        // completed [1]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(2000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));

        // initiated [1,2,3]
        // completed [1]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));

        // initiated [1,2,3,4]
        // completed [1]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(4000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalCompletedTime(4000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(1000l));

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalCompletedTime(2000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (3)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(5000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        // external  (4)
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 4000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(2000l));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        // external  (4)
        globalCompletionTimeStateManager.submitLocalCompletedTime(3000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(4000l));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (4)
        globalCompletionTimeStateManager.submitLocalCompletedTime(5000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(4000l));

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        // external  (5)
        globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 5000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(4000l));

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        // external  (5)
        globalCompletionTimeStateManager.submitLocalInitiatedTime(6000l);
        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(5000l));

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        // external  (4) <-- SHOULD NEVER DECREASE
        boolean exceptionThrown = false;
        try {
            globalCompletionTimeStateManager.submitPeerCompletionTime(otherPeerId, 4000l);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));

        assertThat(globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is(5000l));
    }
}
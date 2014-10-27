package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExternalCompletionTimeTest {
    @Ignore
    @Test
    public void extendInterfaceToExposeMinimumInitiatedTimeAsWell() {
        // TODO at present (i.e., single process mode) this is not needed, but as soon as there are multiple processes this will be necessary, or GCT tracking will have a bug
        // TODO restructure completion time class hierarchy ONCE AGAIN, but this time to be more composeable
        // TODO CT, MultiCT, ConcurrentMultiCT (no need for differentiation between LCT, ECT, GCT at class level)
        assertThat(true, is(false));
    }

    @Test
    public void checkForNonMonotonicallyIncreasingExternalCompletionTimes() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTimeStateManager ect = new ExternalCompletionTimeStateManager(peerIds);

        // When/Then
        assertThat(ect.externalCompletionTimeAsMilli(), is(-1l));

        ect.submitPeerCompletionTime(peerId1, 1);
        assertThat(ect.externalCompletionTimeAsMilli(), is(-1l));

        ect.submitPeerCompletionTime(peerId2, 1);
        assertThat(ect.externalCompletionTimeAsMilli(), is(1l));

        ect.submitPeerCompletionTime(peerId1, 2l);
        assertThat(ect.externalCompletionTimeAsMilli(), is(1l));

        ect.submitPeerCompletionTime(peerId2, 2l);
        assertThat(ect.externalCompletionTimeAsMilli(), is(2l));

        boolean exceptionThrown = false;
        try {
            ect.submitPeerCompletionTime(peerId2, 1);
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
        assertThat(ect.externalCompletionTimeAsMilli(), is(2l));
    }

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenPeerIdListContainsNullValue() throws CompletionTimeException {
        // Given
        Set<String> peerIds = new HashSet<>();
        peerIds.add(null);
        ExternalCompletionTimeStateManager ect = new ExternalCompletionTimeStateManager(peerIds);

        // When
        // no events have been applied

        // Then
        // should never get to this line
    }


    @Test
    public void shouldReturnNullWhenNoCompletionTimesHaveBeenApplied() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTimeStateManager ect = new ExternalCompletionTimeStateManager(peerIds);

        // When
        // no events have been applied

        // Then
        assertThat(ect.externalCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldReturnNullWhenCompletionTimeHasNotBeenAppliedForOneOrMorePeers() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTimeStateManager ect = new ExternalCompletionTimeStateManager(peerIds);

        // When
        ect.submitPeerCompletionTime(peerId1, 1);

        // Then
        assertThat(ect.externalCompletionTimeAsMilli(), is(-1l));
    }

    @Test
    public void shouldReturnMinimumOfAllPeersCompletionTime() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTimeStateManager ect = new ExternalCompletionTimeStateManager(peerIds);

        // When
        ect.submitPeerCompletionTime(peerId1, 1);
        ect.submitPeerCompletionTime(peerId2, 2l);

        // Then
        assertThat(ect.externalCompletionTimeAsMilli(), equalTo(1l));
    }

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenNullPeerIdApplied() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTimeStateManager ect = new ExternalCompletionTimeStateManager(peerIds);

        // When
        ect.submitPeerCompletionTime(null, 1);

        // Then
        // should never get to this line
    }

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenNullCompletionTimeIsApplied() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTimeStateManager ect = new ExternalCompletionTimeStateManager(peerIds);

        // When
        ect.submitPeerCompletionTime(peerId1, -1);

        // Then
        // should never get to this line
    }
}

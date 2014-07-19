package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class ExternalCompletionTimeTest {
    @Test
    public void checkForNonMonotonicallyIncreasingExternalCompletionTimes() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When/Then
        assertThat(ect.completionTime(), is(nullValue()));
        ect.applyPeerCompletionTime(peerId1, Time.fromMilli(1));
        assertThat(ect.completionTime(), is(nullValue()));
        ect.applyPeerCompletionTime(peerId2, Time.fromMilli(1));
        assertThat(ect.completionTime(), is(Time.fromMilli(1)));

        ect.applyPeerCompletionTime(peerId1, Time.fromMilli(2));
        assertThat(ect.completionTime(), is(Time.fromMilli(1)));
        ect.applyPeerCompletionTime(peerId2, Time.fromMilli(2));
        assertThat(ect.completionTime(), is(Time.fromMilli(2)));

        boolean exceptionThrown = false;
        try {
            ect.applyPeerCompletionTime(peerId2, Time.fromMilli(1));
        } catch (CompletionTimeException e) {
            exceptionThrown = true;
        }
        assertThat(ect.completionTime(), is(Time.fromMilli(2)));
    }

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenPeerIdListContainsNullValue() throws CompletionTimeException {
        // Given
        Set<String> peerIds = new HashSet<>();
        peerIds.add(null);
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

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
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        // no events have been applied

        // Then
        assertThat(ect.completionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenCompletionTimeHasNotBeenAppliedForOneOrMorePeers() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        ect.applyPeerCompletionTime(peerId1, Time.fromMilli(1));

        // Then
        assertThat(ect.completionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnMinimumOfAllPeersCompletionTime() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        ect.applyPeerCompletionTime(peerId1, Time.fromMilli(1));
        ect.applyPeerCompletionTime(peerId2, Time.fromMilli(2));

        // Then
        assertThat(ect.completionTime(), equalTo(Time.fromMilli(1)));
    }

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenNullPeerIdApplied() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        ect.applyPeerCompletionTime(null, Time.fromMilli(1));

        // Then
        // should never get to this line
    }

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenNullCompletionTimeIsApplied() throws CompletionTimeException {
        // Given
        String peerId1 = "peerId1";
        String peerId2 = "peerId2";
        Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        ect.applyPeerCompletionTime(peerId1, null);

        // Then
        // should never get to this line
    }
}

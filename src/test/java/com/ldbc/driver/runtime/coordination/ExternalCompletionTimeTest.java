package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class ExternalCompletionTimeTest {
    String peerId1 = "peerId1";
    String peerId2 = "peerId2";
    Set<String> peerIds = Sets.newHashSet(peerId1, peerId2);

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenPeerIdListContainsNullValue() throws CompletionTimeException {
        // Given
        ExternalCompletionTime ect = new ExternalCompletionTime(Sets.<String>newHashSet(null));

        // When
        // no events have been applied

        // Then
        // should never get to this line
    }


    @Test
    public void shouldReturnNullWhenNoCompletionTimesHaveBeenApplied() throws CompletionTimeException {
        // Given
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        // no events have been applied

        // Then
        assertThat(ect.completionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenCompletionTimeHasNotBeenAppliedForOneOrMorePeers() throws CompletionTimeException {
        // Given
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        ect.applyPeerCompletionTime(peerId1, Time.fromMilli(1));

        // Then
        assertThat(ect.completionTime(), is(nullValue()));
    }

    @Test
    public void shouldReturnMinimumOfAllPeersCompletionTime() throws CompletionTimeException {
        // Given
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
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        ect.applyPeerCompletionTime(null, Time.fromMilli(1));

        // Then
        // should never get to this line
    }

    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenNullCompletionTimeIsApplied() throws CompletionTimeException {
        // Given
        ExternalCompletionTime ect = new ExternalCompletionTime(peerIds);

        // When
        ect.applyPeerCompletionTime(peerId1, null);

        // Then
        // should never get to this line
    }
}

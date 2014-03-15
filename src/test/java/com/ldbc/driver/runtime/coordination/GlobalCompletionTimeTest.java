package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.Time;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

//Completion Time = min( min(Initiated Events), max(Completed Events) )
public class GlobalCompletionTimeTest {
    String otherPeerId = "otherPeer";
    List<String> peerIds = Lists.newArrayList(otherPeerId);
    GlobalCompletionTime gct = null;

    @Before
    public void createCompletionTime() throws CompletionTimeException {
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);
    }

    // LocalIT = none, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        // no events have been initiated or completed

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    // LocalIT = some, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenSomeLocalITNoLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        gct.localCompletionTime().applyInitiatedTime(Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    // LocalIT = none, LocalCT = some, ExternalCT = none --> Exception
    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenNoLocalITSomeLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        gct.localCompletionTime().applyCompletedTime(Time.fromSeconds(1));

        // Then
        // exception should have already been thrown
    }

    //  LocalIT = none, LocalCT = none, ExternalCT = some --> null
    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTSomeExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        gct.externalCompletionTime().applyPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    //  LocalIT = some, LocalCT = some, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenSomeLocalITSomeLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        gct.localCompletionTime().applyInitiatedTime(Time.fromSeconds(1));
        gct.localCompletionTime().applyCompletedTime(Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    //  LocalIT = 1, LocalCT = none, ExternalCT = 2 --> 1
    @Test
    public void shouldReturnLocalITWhenLowerLocalITNoLocalCTHigherExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        gct.localCompletionTime().applyInitiatedTime(Time.fromSeconds(1));
        gct.externalCompletionTime().applyPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        // Then
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 2, LocalCT = none, ExternalCT = 1 --> 1
    @Test
    public void shouldReturnExternalCTWhenHigherLocalITNoLocalCTLowerExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        gct.localCompletionTime().applyInitiatedTime(Time.fromSeconds(2));
        gct.externalCompletionTime().applyPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 1, LocalCT = 1, ExternalCT = 2 --> 1
    @Test
    public void shouldReturnLocalMinWhenLowerLocalITLowerLocalCTHigherExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        gct.localCompletionTime().applyInitiatedTime(Time.fromSeconds(1));
        gct.localCompletionTime().applyCompletedTime(Time.fromSeconds(1));
        gct.externalCompletionTime().applyPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        // Then
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 2, LocalCT = 2, ExternalCT =  --> 1
    @Test
    public void shouldReturnExternalCTWhenHigherLocalITHigherLocalCTLowerExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter

        // When
        gct.localCompletionTime().applyInitiatedTime(Time.fromSeconds(2));
        gct.localCompletionTime().applyCompletedTime(Time.fromSeconds(2));
        gct.externalCompletionTime().applyPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
    }
}

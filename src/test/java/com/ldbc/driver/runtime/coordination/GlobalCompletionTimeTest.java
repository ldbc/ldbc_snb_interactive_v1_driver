package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

//Completion Time = min( min(Initiated Events), max(Completed Events) )
public class GlobalCompletionTimeTest {
    final String otherPeerId = "otherPeer";
    final List<String> peerIds = Lists.newArrayList(otherPeerId);

    // LocalIT = none, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
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
    public void shouldReturnNullWhenSomeLocalITNoLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    // LocalIT = none, LocalCT = some, ExternalCT = none --> Exception
    public void shouldThrowExceptionWhenNoLocalITSomeLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
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
    public void shouldReturnNullWhenNoLocalITNoLocalCTSomeExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
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
    public void shouldReturnNullWhenSomeLocalITSomeLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(1));
        gct.applyCompletedTime(Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(nullValue()));
    }

    //  LocalIT = 1, LocalCT = none, ExternalCT = 2 --> 1
    @Test
    public void shouldReturnLocalITWhenLowerLocalITNoLocalCTHigherExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(1));
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(2));

        // Then
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 2, LocalCT = none, ExternalCT = 1 --> 1
    @Test
    public void shouldReturnExternalCTWhenHigherLocalITNoLocalCTLowerExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
        LocalCompletionTime localCompletionTime = new LocalCompletionTime();
        ExternalCompletionTime externalCompletionTime = new ExternalCompletionTime(peerIds);
        GlobalCompletionTime gct = new GlobalCompletionTime(localCompletionTime, externalCompletionTime);

        // When
        gct.applyInitiatedTime(Time.fromSeconds(2));
        gct.applyPeerCompletionTime(otherPeerId, Time.fromSeconds(1));

        // Then
        assertThat(gct.completionTime(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 1, LocalCT = 1, ExternalCT = 2 --> 1
    @Test
    public void shouldReturnLocalMinWhenLowerLocalITLowerLocalCTHigherExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
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
    public void shouldReturnExternalCTWhenHigherLocalITHigherLocalCTLowerExternalCT() throws CompletionTimeException {
        // Given
        // gct parameter
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
}
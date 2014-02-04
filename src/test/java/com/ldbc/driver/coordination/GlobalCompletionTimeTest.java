package com.ldbc.driver.coordination;

import com.ldbc.driver.temporal.Time;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

//Completion Time = min( min(Initiated Events), max(Completed Events) )
public class GlobalCompletionTimeTest {
    GlobalCompletionTime ct = null;

    @Before
    public void createCompletionTime() {
        ct = new GlobalCompletionTime();
    }

    // LocalIT = none, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        // no events have been initiated or completed

        // Then
        assertThat(ct.get(), is(nullValue()));
    }

    // LocalIT = some, LocalCT = none, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenSomeLocalITNoLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        ct.applyInitiatedTime(Time.fromSeconds(1));

        // Then
        assertThat(ct.get(), is(nullValue()));
    }

    // LocalIT = none, LocalCT = some, ExternalCT = none --> Exception
    @Test(expected = CompletionTimeException.class)
    public void shouldThrowExceptionWhenNoLocalITSomeLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        ct.applyCompletedTime(Time.fromSeconds(1));

        // Then
        // exception should have already been thrown
    }

    //  LocalIT = none, LocalCT = none, ExternalCT = some --> null
    @Test
    public void shouldReturnNullWhenNoLocalITNoLocalCTSomeExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        ct.applyExternalCompletionTime(Time.fromSeconds(1));

        // Then
        assertThat(ct.get(), is(nullValue()));
    }

    //  LocalIT = some, LocalCT = some, ExternalCT = none --> null
    @Test
    public void shouldReturnNullWhenSomeLocalITSomeLocalCTNoExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        ct.applyInitiatedTime(Time.fromSeconds(1));
        ct.applyCompletedTime(Time.fromSeconds(1));

        // Then
        assertThat(ct.get(), is(nullValue()));
    }

    //  LocalIT = 1, LocalCT = none, ExternalCT = 2 --> 1
    @Test
    public void shouldReturnLocalITWhenLowerLocalITNoLocalCTHigherExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        ct.applyInitiatedTime(Time.fromSeconds(1));
        ct.applyExternalCompletionTime(Time.fromSeconds(2));

        // Then
        assertThat(ct.get(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 2, LocalCT = none, ExternalCT = 1 --> 1
    @Test
    public void shouldReturnExternalCTWhenHigherLocalITNoLocalCTLowerExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        ct.applyInitiatedTime(Time.fromSeconds(2));
        ct.applyExternalCompletionTime(Time.fromSeconds(1));

        // Then
        assertThat(ct.get(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 1, LocalCT = 1, ExternalCT = 2 --> 1
    @Test
    public void shouldReturnLocalMinWhenLowerLocalITLowerLocalCTHigherExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        ct.applyInitiatedTime(Time.fromSeconds(1));
        ct.applyCompletedTime(Time.fromSeconds(1));
        ct.applyExternalCompletionTime(Time.fromSeconds(2));

        // Then
        assertThat(ct.get(), is(Time.fromSeconds(1)));
    }

    //  LocalIT = 2, LocalCT = 2, ExternalCT =  --> 1
    @Test
    public void shouldReturnExternalCTWhenHigherLocalITHigherLocalCTLowerExternalCT() throws CompletionTimeException {
        // Given
        // ct parameter

        // When
        ct.applyInitiatedTime(Time.fromSeconds(2));
        ct.applyCompletedTime(Time.fromSeconds(2));
        ct.applyExternalCompletionTime(Time.fromSeconds(1));

        // Then
        assertThat(ct.get(), is(Time.fromSeconds(1)));
    }
}

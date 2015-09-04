package com.ldbc.driver.workloads;

import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.DriverConfigurationException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class OperationMixTest
{
    @Test
    public void equalityShouldWorkForFrequencies() throws WorkloadException, DriverConfigurationException
    {
        long baseIntervalAsMilli = 10;
        OperationMixBuilder.OperationMix operationMix1 = OperationMixBuilder.fromFrequencies( baseIntervalAsMilli )
                .addOperationFrequency( 1, 10 )
                .addOperationFrequency( 2, 20 )
                .addOperationFrequency( 3, 30 )
                .addOperationFrequency( 4, 40 )
                .addOperationFrequency( 5, 50 )
                .build();

        OperationMixBuilder.OperationMix operationMix2 = OperationMixBuilder.fromFrequencies( baseIntervalAsMilli )
                .addOperationFrequency( 1, 10 )
                .addOperationFrequency( 2, 20 )
                .addOperationFrequency( 3, 30 )
                .addOperationFrequency( 4, 40 )
                .addOperationFrequency( 5, 50 )
                .build();

        assertThat( operationMix1, equalTo( operationMix2 ) );
    }

    @Test
    public void equalityShouldWorkForInterleaves() throws WorkloadException, DriverConfigurationException
    {
        OperationMixBuilder.OperationMix operationMix1 = OperationMixBuilder.fromInterleaves()
                .addOperationInterleave( 1, 10 )
                .addOperationInterleave( 2, 20 )
                .addOperationInterleave( 3, 30 )
                .addOperationInterleave( 4, 40 )
                .addOperationInterleave( 5, 50 )
                .build();

        OperationMixBuilder.OperationMix operationMix2 = OperationMixBuilder.fromInterleaves()
                .addOperationInterleave( 1, 10 )
                .addOperationInterleave( 2, 20 )
                .addOperationInterleave( 3, 30 )
                .addOperationInterleave( 4, 40 )
                .addOperationInterleave( 5, 50 )
                .build();

        assertThat( operationMix1, equalTo( operationMix2 ) );
    }

    @Test
    public void shouldConvertFromInterleaveToFrequency() throws WorkloadException, DriverConfigurationException
    {
        OperationMixBuilder.OperationMix operationMix1 = OperationMixBuilder.fromInterleaves()
                .addOperationInterleave( 1, 10 )
                .addOperationInterleave( 2, 20 )
                .addOperationInterleave( 3, 30 )
                .addOperationInterleave( 4, 40 )
                .addOperationInterleave( 5, 50 )
                .build();

        long baseIntervalAsMilli = 10;
        OperationMixBuilder.OperationMix operationMix2 = OperationMixBuilder.fromFrequencies( baseIntervalAsMilli )
                .addOperationFrequency( 1, 1 )
                .addOperationFrequency( 2, 2 )
                .addOperationFrequency( 3, 3 )
                .addOperationFrequency( 4, 4 )
                .addOperationFrequency( 5, 5 )
                .build();

        assertThat( operationMix1, equalTo( operationMix2 ) );
    }

    @Test
    public void shouldRetrieveIndividualInterleavesFromInterleaves() throws WorkloadException,
            DriverConfigurationException
    {
        OperationMixBuilder.OperationMix operationMix = OperationMixBuilder.fromInterleaves()
                .addOperationInterleave( 1, 10 )
                .addOperationInterleave( 2, 20 )
                .addOperationInterleave( 3, 30 )
                .addOperationInterleave( 4, 40 )
                .addOperationInterleave( 5, 50 )
                .build();

        assertThat( operationMix.interleaveFor( 1 ), equalTo( 10l ) );
        assertThat( operationMix.interleaveFor( 2 ), equalTo( 20l ) );
        assertThat( operationMix.interleaveFor( 3 ), equalTo( 30l ) );
        assertThat( operationMix.interleaveFor( 4 ), equalTo( 40l ) );
        assertThat( operationMix.interleaveFor( 5 ), equalTo( 50l ) );

        Map<Integer,Long> expectedInterleaves = new HashMap<>();
        expectedInterleaves.put( 1, 10l );
        expectedInterleaves.put( 2, 20l );
        expectedInterleaves.put( 3, 30l );
        expectedInterleaves.put( 4, 40l );
        expectedInterleaves.put( 5, 50l );
        assertThat( operationMix.interleaves(), equalTo( expectedInterleaves ) );
    }

    @Test
    public void shouldRetrieveIndividualInterleavesFromFrequencies() throws WorkloadException,
            DriverConfigurationException
    {
        OperationMixBuilder.OperationMix operationMix = OperationMixBuilder.fromFrequencies( 10 )
                .addOperationFrequency( 1, 1 )
                .addOperationFrequency( 2, 2 )
                .addOperationFrequency( 3, 3 )
                .addOperationFrequency( 4, 4 )
                .addOperationFrequency( 5, 5 )
                .build();

        assertThat( operationMix.interleaveFor( 1 ), equalTo( 10l ) );
        assertThat( operationMix.interleaveFor( 2 ), equalTo( 20l ) );
        assertThat( operationMix.interleaveFor( 3 ), equalTo( 30l ) );
        assertThat( operationMix.interleaveFor( 4 ), equalTo( 40l ) );
        assertThat( operationMix.interleaveFor( 5 ), equalTo( 50l ) );

        Map<Integer,Long> expectedInterleaves = new HashMap<>();
        expectedInterleaves.put( 1, 10l );
        expectedInterleaves.put( 2, 20l );
        expectedInterleaves.put( 3, 30l );
        expectedInterleaves.put( 4, 40l );
        expectedInterleaves.put( 5, 50l );
        assertThat( operationMix.interleaves(), equalTo( expectedInterleaves ) );
    }
}

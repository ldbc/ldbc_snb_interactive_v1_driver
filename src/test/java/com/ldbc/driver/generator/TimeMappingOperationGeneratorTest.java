package com.ldbc.driver.generator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TimeMappingOperationGeneratorTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private TimeSource timeSource = new SystemTimeSource();
    private final long RANDOM_SEED = 42;
    private GeneratorFactory gf = null;

    @Before
    public final void initGeneratorFactory()
    {
        gf = new GeneratorFactory( new RandomDataGeneratorFactory( RANDOM_SEED ) );
    }

    @Test
    public void shouldOffset()
    {
        // Given
        Iterator<Operation> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        // start times
                        gf.incrementing( 0l, 100l ),
                        // dependency times
                        gf.incrementing( 0l, 50l ),
                        // names
                        gf.constant( "name1" )
                ),
                11
        );
        List<Operation> operationsList = ImmutableList.copyOf( operations );
        assertThat( operationsList.size(), is( 11 ) );
        assertThat( operationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 0l ) );
        assertThat( operationsList.get( 0 ).timeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 0 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 100l ) );
        assertThat( operationsList.get( 1 ).timeStamp(), equalTo( 100l ) );
        assertThat( operationsList.get( 1 ).dependencyTimeStamp(), equalTo( 50l ) );
        assertThat( operationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 200l ) );
        assertThat( operationsList.get( 2 ).timeStamp(), equalTo( 200l ) );
        assertThat( operationsList.get( 2 ).dependencyTimeStamp(), equalTo( 100l ) );
        assertThat( operationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 300l ) );
        assertThat( operationsList.get( 3 ).timeStamp(), equalTo( 300l ) );
        assertThat( operationsList.get( 3 ).dependencyTimeStamp(), equalTo( 150l ) );
        assertThat( operationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 400l ) );
        assertThat( operationsList.get( 4 ).timeStamp(), equalTo( 400l ) );
        assertThat( operationsList.get( 4 ).dependencyTimeStamp(), equalTo( 200l ) );
        assertThat( operationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 500l ) );
        assertThat( operationsList.get( 5 ).timeStamp(), equalTo( 500l ) );
        assertThat( operationsList.get( 5 ).dependencyTimeStamp(), equalTo( 250l ) );
        assertThat( operationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 600l ) );
        assertThat( operationsList.get( 6 ).timeStamp(), equalTo( 600l ) );
        assertThat( operationsList.get( 6 ).dependencyTimeStamp(), equalTo( 300l ) );
        assertThat( operationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 700l ) );
        assertThat( operationsList.get( 7 ).timeStamp(), equalTo( 700l ) );
        assertThat( operationsList.get( 7 ).dependencyTimeStamp(), equalTo( 350l ) );
        assertThat( operationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 800l ) );
        assertThat( operationsList.get( 8 ).timeStamp(), equalTo( 800l ) );
        assertThat( operationsList.get( 8 ).dependencyTimeStamp(), equalTo( 400l ) );
        assertThat( operationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 900l ) );
        assertThat( operationsList.get( 9 ).timeStamp(), equalTo( 900l ) );
        assertThat( operationsList.get( 9 ).dependencyTimeStamp(), equalTo( 450l ) );
        assertThat( operationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 1000l ) );
        assertThat( operationsList.get( 10 ).timeStamp(), equalTo( 1000l ) );
        assertThat( operationsList.get( 10 ).dependencyTimeStamp(), equalTo( 500l ) );

        // When
        long newStartTime = 500l;
        List<Operation> offsetOperationsList =
                ImmutableList.copyOf( gf.timeOffset( operationsList.iterator(), newStartTime ) );

        // Then
        assertThat( offsetOperationsList.size(), is( 11 ) );
        assertThat( offsetOperationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 500l ) );
        assertThat( offsetOperationsList.get( 0 ).timeStamp(), equalTo( 0l ) );
        assertThat( offsetOperationsList.get( 0 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetOperationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 600l ) );
        assertThat( offsetOperationsList.get( 1 ).timeStamp(), equalTo( 100l ) );
        assertThat( offsetOperationsList.get( 1 ).dependencyTimeStamp(), equalTo( 50l ) );
        assertThat( offsetOperationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 700l ) );
        assertThat( offsetOperationsList.get( 2 ).timeStamp(), equalTo( 200l ) );
        assertThat( offsetOperationsList.get( 2 ).dependencyTimeStamp(), equalTo( 100l ) );
        assertThat( offsetOperationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 800l ) );
        assertThat( offsetOperationsList.get( 3 ).timeStamp(), equalTo( 300l ) );
        assertThat( offsetOperationsList.get( 3 ).dependencyTimeStamp(), equalTo( 150l ) );
        assertThat( offsetOperationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 900l ) );
        assertThat( offsetOperationsList.get( 4 ).timeStamp(), equalTo( 400l ) );
        assertThat( offsetOperationsList.get( 4 ).dependencyTimeStamp(), equalTo( 200l ) );
        assertThat( offsetOperationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 1000l ) );
        assertThat( offsetOperationsList.get( 5 ).timeStamp(), equalTo( 500l ) );
        assertThat( offsetOperationsList.get( 5 ).dependencyTimeStamp(), equalTo( 250l ) );
        assertThat( offsetOperationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 1100l ) );
        assertThat( offsetOperationsList.get( 6 ).timeStamp(), equalTo( 600l ) );
        assertThat( offsetOperationsList.get( 6 ).dependencyTimeStamp(), equalTo( 300l ) );
        assertThat( offsetOperationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 1200l ) );
        assertThat( offsetOperationsList.get( 7 ).timeStamp(), equalTo( 700l ) );
        assertThat( offsetOperationsList.get( 7 ).dependencyTimeStamp(), equalTo( 350l ) );
        assertThat( offsetOperationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 1300l ) );
        assertThat( offsetOperationsList.get( 8 ).timeStamp(), equalTo( 800l ) );
        assertThat( offsetOperationsList.get( 8 ).dependencyTimeStamp(), equalTo( 400l ) );
        assertThat( offsetOperationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 1400l ) );
        assertThat( offsetOperationsList.get( 9 ).timeStamp(), equalTo( 900l ) );
        assertThat( offsetOperationsList.get( 9 ).dependencyTimeStamp(), equalTo( 450l ) );
        assertThat( offsetOperationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 1500l ) );
        assertThat( offsetOperationsList.get( 10 ).timeStamp(), equalTo( 1000l ) );
        assertThat( offsetOperationsList.get( 10 ).dependencyTimeStamp(), equalTo( 500l ) );
    }

    @Test
    public void shouldOffsetAndCompress()
    {
        // Given
        Iterator<Operation> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        // start times
                        gf.incrementing( 1000l, 100l ),
                        // dependency times
                        gf.incrementing( 900l, 50l ),
                        // names
                        gf.constant( "name1" )
                ),
                11
        );
        List<Operation> operationsList = ImmutableList.copyOf( operations );

        assertThat( operationsList.size(), is( 11 ) );
        assertThat( operationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 1000l ) );
        assertThat( operationsList.get( 0 ).timeStamp(), equalTo( 1000l ) );
        assertThat( operationsList.get( 0 ).dependencyTimeStamp(), equalTo( 900l ) );
        assertThat( operationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 1100l ) );
        assertThat( operationsList.get( 1 ).timeStamp(), equalTo( 1100l ) );
        assertThat( operationsList.get( 1 ).dependencyTimeStamp(), equalTo( 950l ) );
        assertThat( operationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 1200l ) );
        assertThat( operationsList.get( 2 ).timeStamp(), equalTo( 1200l ) );
        assertThat( operationsList.get( 2 ).dependencyTimeStamp(), equalTo( 1000l ) );
        assertThat( operationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 1300l ) );
        assertThat( operationsList.get( 3 ).timeStamp(), equalTo( 1300l ) );
        assertThat( operationsList.get( 3 ).dependencyTimeStamp(), equalTo( 1050l ) );
        assertThat( operationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 1400l ) );
        assertThat( operationsList.get( 4 ).timeStamp(), equalTo( 1400l ) );
        assertThat( operationsList.get( 4 ).dependencyTimeStamp(), equalTo( 1100l ) );
        assertThat( operationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 1500l ) );
        assertThat( operationsList.get( 5 ).timeStamp(), equalTo( 1500l ) );
        assertThat( operationsList.get( 5 ).dependencyTimeStamp(), equalTo( 1150l ) );
        assertThat( operationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 1600l ) );
        assertThat( operationsList.get( 6 ).timeStamp(), equalTo( 1600l ) );
        assertThat( operationsList.get( 6 ).dependencyTimeStamp(), equalTo( 1200l ) );
        assertThat( operationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 1700l ) );
        assertThat( operationsList.get( 7 ).timeStamp(), equalTo( 1700l ) );
        assertThat( operationsList.get( 7 ).dependencyTimeStamp(), equalTo( 1250l ) );
        assertThat( operationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 1800l ) );
        assertThat( operationsList.get( 8 ).timeStamp(), equalTo( 1800l ) );
        assertThat( operationsList.get( 8 ).dependencyTimeStamp(), equalTo( 1300l ) );
        assertThat( operationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 1900l ) );
        assertThat( operationsList.get( 9 ).timeStamp(), equalTo( 1900l ) );
        assertThat( operationsList.get( 9 ).dependencyTimeStamp(), equalTo( 1350l ) );
        assertThat( operationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 2000l ) );
        assertThat( operationsList.get( 10 ).timeStamp(), equalTo( 2000l ) );
        assertThat( operationsList.get( 10 ).dependencyTimeStamp(), equalTo( 1400l ) );

        // When
        long newStartTime = 500l;
        Double compressionRatio = 0.2;
        List<Operation> offsetAndCompressedOperationsList = ImmutableList.copyOf(
                gf.timeOffsetAndCompress(
                        operationsList.iterator(),
                        newStartTime,
                        compressionRatio
                )
        );

        // Then
        assertThat( offsetAndCompressedOperationsList.size(), is( 11 ) );
        assertThat( offsetAndCompressedOperationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 500l ) );
        assertThat( offsetAndCompressedOperationsList.get( 0 ).timeStamp(), equalTo( 1000l ) );
        assertThat( offsetAndCompressedOperationsList.get( 0 ).dependencyTimeStamp(), equalTo( 900l ) );
        assertThat( offsetAndCompressedOperationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 520l ) );
        assertThat( offsetAndCompressedOperationsList.get( 1 ).timeStamp(), equalTo( 1100l ) );
        assertThat( offsetAndCompressedOperationsList.get( 1 ).dependencyTimeStamp(), equalTo( 950l ) );
        assertThat( offsetAndCompressedOperationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 540l ) );
        assertThat( offsetAndCompressedOperationsList.get( 2 ).timeStamp(), equalTo( 1200l ) );
        assertThat( offsetAndCompressedOperationsList.get( 2 ).dependencyTimeStamp(), equalTo( 1000l ) );
        assertThat( offsetAndCompressedOperationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 560l ) );
        assertThat( offsetAndCompressedOperationsList.get( 3 ).timeStamp(), equalTo( 1300l ) );
        assertThat( offsetAndCompressedOperationsList.get( 3 ).dependencyTimeStamp(), equalTo( 1050l ) );
        assertThat( offsetAndCompressedOperationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 580l ) );
        assertThat( offsetAndCompressedOperationsList.get( 4 ).timeStamp(), equalTo( 1400l ) );
        assertThat( offsetAndCompressedOperationsList.get( 4 ).dependencyTimeStamp(), equalTo( 1100l ) );
        assertThat( offsetAndCompressedOperationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 600l ) );
        assertThat( offsetAndCompressedOperationsList.get( 5 ).timeStamp(), equalTo( 1500l ) );
        assertThat( offsetAndCompressedOperationsList.get( 5 ).dependencyTimeStamp(), equalTo( 1150l ) );
        assertThat( offsetAndCompressedOperationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 620l ) );
        assertThat( offsetAndCompressedOperationsList.get( 6 ).timeStamp(), equalTo( 1600l ) );
        assertThat( offsetAndCompressedOperationsList.get( 6 ).dependencyTimeStamp(), equalTo( 1200l ) );
        assertThat( offsetAndCompressedOperationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 640l ) );
        assertThat( offsetAndCompressedOperationsList.get( 7 ).timeStamp(), equalTo( 1700l ) );
        assertThat( offsetAndCompressedOperationsList.get( 7 ).dependencyTimeStamp(), equalTo( 1250l ) );
        assertThat( offsetAndCompressedOperationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 660l ) );
        assertThat( offsetAndCompressedOperationsList.get( 8 ).timeStamp(), equalTo( 1800l ) );
        assertThat( offsetAndCompressedOperationsList.get( 8 ).dependencyTimeStamp(), equalTo( 1300l ) );
        assertThat( offsetAndCompressedOperationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 680l ) );
        assertThat( offsetAndCompressedOperationsList.get( 9 ).timeStamp(), equalTo( 1900l ) );
        assertThat( offsetAndCompressedOperationsList.get( 9 ).dependencyTimeStamp(), equalTo( 1350l ) );
        assertThat( offsetAndCompressedOperationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 700l ) );
        assertThat( offsetAndCompressedOperationsList.get( 10 ).timeStamp(), equalTo( 2000l ) );
        assertThat( offsetAndCompressedOperationsList.get( 10 ).dependencyTimeStamp(), equalTo( 1400l ) );
    }

    @Test
    public void shouldOffsetAndCompressWhenTimesAreVeryCloseTogetherWithoutRoundingErrors()
    {
        // Given
        Iterator<Operation> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        // start times
                        gf.incrementing( 0l, 1l ),
                        // dependency times
                        gf.incrementing( 0l, 0l ),
                        // names
                        gf.constant( "name1" )
                ),
                11
        );
        List<Operation> operationsList = ImmutableList.copyOf( operations );

        assertThat( operationsList.size(), is( 11 ) );
        assertThat( operationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 0l ) );
        assertThat( operationsList.get( 0 ).timeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 0 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 1l ) );
        assertThat( operationsList.get( 1 ).timeStamp(), equalTo( 1l ) );
        assertThat( operationsList.get( 1 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 2l ) );
        assertThat( operationsList.get( 2 ).timeStamp(), equalTo( 2l ) );
        assertThat( operationsList.get( 2 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( operationsList.get( 3 ).timeStamp(), equalTo( 3l ) );
        assertThat( operationsList.get( 3 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 4l ) );
        assertThat( operationsList.get( 4 ).timeStamp(), equalTo( 4l ) );
        assertThat( operationsList.get( 4 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 5l ) );
        assertThat( operationsList.get( 5 ).timeStamp(), equalTo( 5l ) );
        assertThat( operationsList.get( 5 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 6l ) );
        assertThat( operationsList.get( 6 ).timeStamp(), equalTo( 6l ) );
        assertThat( operationsList.get( 6 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 7l ) );
        assertThat( operationsList.get( 7 ).timeStamp(), equalTo( 7l ) );
        assertThat( operationsList.get( 7 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 8l ) );
        assertThat( operationsList.get( 8 ).timeStamp(), equalTo( 8l ) );
        assertThat( operationsList.get( 8 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 9l ) );
        assertThat( operationsList.get( 9 ).timeStamp(), equalTo( 9l ) );
        assertThat( operationsList.get( 9 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( operationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 10l ) );
        assertThat( operationsList.get( 10 ).timeStamp(), equalTo( 10l ) );
        assertThat( operationsList.get( 10 ).dependencyTimeStamp(), equalTo( 0l ) );

        // When
        long newStartTime = 0l;
        Double compressionRatio = 0.5;
        List<Operation> offsetAndCompressedOperations = ImmutableList
                .copyOf( gf.timeOffsetAndCompress( operationsList.iterator(), newStartTime, compressionRatio ) );

        // Then
        assertThat( offsetAndCompressedOperations.size(), is( 11 ) );
        assertThat( offsetAndCompressedOperations.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 0 ).timeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 0 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 1l ) );
        assertThat( offsetAndCompressedOperations.get( 1 ).timeStamp(), equalTo( 1l ) );
        assertThat( offsetAndCompressedOperations.get( 1 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 1l ) );
        assertThat( offsetAndCompressedOperations.get( 2 ).timeStamp(), equalTo( 2l ) );
        assertThat( offsetAndCompressedOperations.get( 2 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 2l ) );
        assertThat( offsetAndCompressedOperations.get( 3 ).timeStamp(), equalTo( 3l ) );
        assertThat( offsetAndCompressedOperations.get( 3 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 2l ) );
        assertThat( offsetAndCompressedOperations.get( 4 ).timeStamp(), equalTo( 4l ) );
        assertThat( offsetAndCompressedOperations.get( 4 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( offsetAndCompressedOperations.get( 5 ).timeStamp(), equalTo( 5l ) );
        assertThat( offsetAndCompressedOperations.get( 5 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( offsetAndCompressedOperations.get( 6 ).timeStamp(), equalTo( 6l ) );
        assertThat( offsetAndCompressedOperations.get( 6 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 4l ) );
        assertThat( offsetAndCompressedOperations.get( 7 ).timeStamp(), equalTo( 7l ) );
        assertThat( offsetAndCompressedOperations.get( 7 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 4l ) );
        assertThat( offsetAndCompressedOperations.get( 8 ).timeStamp(), equalTo( 8l ) );
        assertThat( offsetAndCompressedOperations.get( 8 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 5l ) );
        assertThat( offsetAndCompressedOperations.get( 9 ).timeStamp(), equalTo( 9l ) );
        assertThat( offsetAndCompressedOperations.get( 9 ).dependencyTimeStamp(), equalTo( 0l ) );
        assertThat( offsetAndCompressedOperations.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 5l ) );
        assertThat( offsetAndCompressedOperations.get( 10 ).timeStamp(), equalTo( 10l ) );
        assertThat( offsetAndCompressedOperations.get( 10 ).dependencyTimeStamp(), equalTo( 0l ) );
    }

    @Test
    public void shouldNotBreakTheMonotonicallyIncreasingScheduledStartTimesOfOperationsFromLdbcWorkload()
            throws WorkloadException, IOException, DriverConfigurationException
    {
        Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
        // Driver-specific parameters
        String name = "name";
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 100;
        int threadCount = 1;
        int statusDisplayInterval = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
        double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        long warmupCount = 0;
        long skipCount = 0;

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                name,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                warmupCount,
                skipCount
        );

        Map<String,String> updateStreamParams = MapUtils.loadPropertiesToMap(
                TestUtils.getResource( "/snb/interactive/updateStream.properties" )
        );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( updateStreamParams );

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init( configuration );

        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
        List<Operation> operations = Lists.newArrayList(
                gf.limit(
                        WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators( gf,
                                workload.streams( gf, false ) ),
                        configuration.operationCount()
                )
        );
        long prevOperationScheduledStartTime = operations.get( 0 ).scheduledStartTimeAsMilli() - 1;
        for ( Operation operation : operations )
        {
            assertThat( operation.scheduledStartTimeAsMilli() >= prevOperationScheduledStartTime, is( true ) );
            prevOperationScheduledStartTime = operation.scheduledStartTimeAsMilli();
        }

        List<Operation> offsetOperations =
                Lists.newArrayList( gf.timeOffset( operations.iterator(), timeSource.nowAsMilli() + 500 ) );
        long prevOffsetOperationScheduledStartTime = offsetOperations.get( 0 ).scheduledStartTimeAsMilli() - 1;
        for ( Operation operation : offsetOperations )
        {
            assertThat( operation.scheduledStartTimeAsMilli() >= prevOffsetOperationScheduledStartTime, is( true ) );
            prevOffsetOperationScheduledStartTime = operation.scheduledStartTimeAsMilli();
        }
        workload.close();
    }

    @Test
    public void shouldAlwaysProduceTheSameOutputWhenGivenTheSameInput()
    {
        // Given
        List<Operation> operations = Lists.<Operation>newArrayList(
                new TimedNamedOperation2( 10l, 10l, 0l, "name2" ),
                new TimedNamedOperation2( 11l, 11l, 1l, "name2" ),
                new TimedNamedOperation1( 12l, 12l, 2l, "name1" )
        );

        long now = new SystemTimeSource().nowAsMilli();

        List<Operation> offsetOperations1 = Lists.newArrayList( gf.timeOffset( operations.iterator(), now ) );
        List<Operation> offsetOperations2 = Lists.newArrayList( gf.timeOffset( operations.iterator(), now ) );
        List<Operation> offsetOperations3 = Lists.newArrayList( gf.timeOffset( operations.iterator(), now ) );
        List<Operation> offsetOperations4 = Lists.newArrayList( gf.timeOffset( operations.iterator(), now ) );

        // When
        GeneratorFactory.OperationStreamComparisonResult stream1Stream2Comparison =
                gf.compareOperationStreams( offsetOperations1.iterator(), offsetOperations2.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream1Stream3Comparison =
                gf.compareOperationStreams( offsetOperations1.iterator(), offsetOperations3.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream1Stream4Comparison =
                gf.compareOperationStreams( offsetOperations1.iterator(), offsetOperations4.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream2Stream1Comparison =
                gf.compareOperationStreams( offsetOperations2.iterator(), offsetOperations1.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream2Stream3Comparison =
                gf.compareOperationStreams( offsetOperations2.iterator(), offsetOperations3.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream2Stream4Comparison =
                gf.compareOperationStreams( offsetOperations2.iterator(), offsetOperations4.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream3Stream1Comparison =
                gf.compareOperationStreams( offsetOperations3.iterator(), offsetOperations1.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream3Stream2Comparison =
                gf.compareOperationStreams( offsetOperations3.iterator(), offsetOperations2.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream3Stream4Comparison =
                gf.compareOperationStreams( offsetOperations3.iterator(), offsetOperations4.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream4Stream1Comparison =
                gf.compareOperationStreams( offsetOperations4.iterator(), offsetOperations1.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream4Stream2Comparison =
                gf.compareOperationStreams( offsetOperations4.iterator(), offsetOperations2.iterator(), true );
        GeneratorFactory.OperationStreamComparisonResult stream4Stream3Comparison =
                gf.compareOperationStreams( offsetOperations4.iterator(), offsetOperations3.iterator(), true );


        // Then
        assertThat( stream1Stream2Comparison.errorMessage(), stream1Stream2Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream1Stream3Comparison.errorMessage(), stream1Stream3Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream1Stream4Comparison.errorMessage(), stream1Stream4Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream2Stream1Comparison.errorMessage(), stream2Stream1Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream2Stream3Comparison.errorMessage(), stream2Stream3Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream2Stream4Comparison.errorMessage(), stream2Stream4Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream3Stream1Comparison.errorMessage(), stream3Stream1Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream3Stream2Comparison.errorMessage(), stream3Stream2Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream3Stream4Comparison.errorMessage(), stream3Stream4Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream4Stream1Comparison.errorMessage(), stream4Stream1Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream4Stream2Comparison.errorMessage(), stream4Stream2Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
        assertThat( stream4Stream3Comparison.errorMessage(), stream4Stream3Comparison.resultType(),
                is( GeneratorFactory.OperationStreamComparisonResultType.PASS ) );
    }
}
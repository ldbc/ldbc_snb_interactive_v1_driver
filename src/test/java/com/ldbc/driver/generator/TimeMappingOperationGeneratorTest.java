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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
                        gf.incrementing( 0L, 100L ),
                        // dependency times
                        gf.incrementing( 0L, 50L ),
                        // names
                        gf.constant( "name1" )
                ),
                11
        );
        List<Operation> operationsList = ImmutableList.copyOf( operations );
        assertThat( operationsList.size(), is( 11 ) );
        assertThat( operationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 0L ) );
        assertThat( operationsList.get( 0 ).timeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 0 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 100L ) );
        assertThat( operationsList.get( 1 ).timeStamp(), equalTo( 100L ) );
        assertThat( operationsList.get( 1 ).dependencyTimeStamp(), equalTo( 50L ) );
        assertThat( operationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 200L ) );
        assertThat( operationsList.get( 2 ).timeStamp(), equalTo( 200L ) );
        assertThat( operationsList.get( 2 ).dependencyTimeStamp(), equalTo( 100L ) );
        assertThat( operationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 300L ) );
        assertThat( operationsList.get( 3 ).timeStamp(), equalTo( 300L ) );
        assertThat( operationsList.get( 3 ).dependencyTimeStamp(), equalTo( 150L ) );
        assertThat( operationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 400L ) );
        assertThat( operationsList.get( 4 ).timeStamp(), equalTo( 400L ) );
        assertThat( operationsList.get( 4 ).dependencyTimeStamp(), equalTo( 200L ) );
        assertThat( operationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 500L ) );
        assertThat( operationsList.get( 5 ).timeStamp(), equalTo( 500L ) );
        assertThat( operationsList.get( 5 ).dependencyTimeStamp(), equalTo( 250L ) );
        assertThat( operationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 600L ) );
        assertThat( operationsList.get( 6 ).timeStamp(), equalTo( 600L ) );
        assertThat( operationsList.get( 6 ).dependencyTimeStamp(), equalTo( 300L ) );
        assertThat( operationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 700L ) );
        assertThat( operationsList.get( 7 ).timeStamp(), equalTo( 700L ) );
        assertThat( operationsList.get( 7 ).dependencyTimeStamp(), equalTo( 350L ) );
        assertThat( operationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 800L ) );
        assertThat( operationsList.get( 8 ).timeStamp(), equalTo( 800L ) );
        assertThat( operationsList.get( 8 ).dependencyTimeStamp(), equalTo( 400L ) );
        assertThat( operationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 900L ) );
        assertThat( operationsList.get( 9 ).timeStamp(), equalTo( 900L ) );
        assertThat( operationsList.get( 9 ).dependencyTimeStamp(), equalTo( 450L ) );
        assertThat( operationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 1000L ) );
        assertThat( operationsList.get( 10 ).timeStamp(), equalTo( 1000L ) );
        assertThat( operationsList.get( 10 ).dependencyTimeStamp(), equalTo( 500L ) );

        // When
        long newStartTime = 500L;
        List<Operation> offsetOperationsList =
                ImmutableList.copyOf( gf.timeOffset( operationsList.iterator(), newStartTime ) );

        // Then
        assertThat( offsetOperationsList.size(), is( 11 ) );
        assertThat( offsetOperationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 500L ) );
        assertThat( offsetOperationsList.get( 0 ).timeStamp(), equalTo( 0L ) );
        assertThat( offsetOperationsList.get( 0 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetOperationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 600L ) );
        assertThat( offsetOperationsList.get( 1 ).timeStamp(), equalTo( 100L ) );
        assertThat( offsetOperationsList.get( 1 ).dependencyTimeStamp(), equalTo( 50L ) );
        assertThat( offsetOperationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 700L ) );
        assertThat( offsetOperationsList.get( 2 ).timeStamp(), equalTo( 200L ) );
        assertThat( offsetOperationsList.get( 2 ).dependencyTimeStamp(), equalTo( 100L ) );
        assertThat( offsetOperationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 800L ) );
        assertThat( offsetOperationsList.get( 3 ).timeStamp(), equalTo( 300L ) );
        assertThat( offsetOperationsList.get( 3 ).dependencyTimeStamp(), equalTo( 150L ) );
        assertThat( offsetOperationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 900L ) );
        assertThat( offsetOperationsList.get( 4 ).timeStamp(), equalTo( 400L ) );
        assertThat( offsetOperationsList.get( 4 ).dependencyTimeStamp(), equalTo( 200L ) );
        assertThat( offsetOperationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 1000L ) );
        assertThat( offsetOperationsList.get( 5 ).timeStamp(), equalTo( 500L ) );
        assertThat( offsetOperationsList.get( 5 ).dependencyTimeStamp(), equalTo( 250L ) );
        assertThat( offsetOperationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 1100L ) );
        assertThat( offsetOperationsList.get( 6 ).timeStamp(), equalTo( 600L ) );
        assertThat( offsetOperationsList.get( 6 ).dependencyTimeStamp(), equalTo( 300L ) );
        assertThat( offsetOperationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 1200L ) );
        assertThat( offsetOperationsList.get( 7 ).timeStamp(), equalTo( 700L ) );
        assertThat( offsetOperationsList.get( 7 ).dependencyTimeStamp(), equalTo( 350L ) );
        assertThat( offsetOperationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 1300L ) );
        assertThat( offsetOperationsList.get( 8 ).timeStamp(), equalTo( 800L ) );
        assertThat( offsetOperationsList.get( 8 ).dependencyTimeStamp(), equalTo( 400L ) );
        assertThat( offsetOperationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 1400L ) );
        assertThat( offsetOperationsList.get( 9 ).timeStamp(), equalTo( 900L ) );
        assertThat( offsetOperationsList.get( 9 ).dependencyTimeStamp(), equalTo( 450L ) );
        assertThat( offsetOperationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 1500L ) );
        assertThat( offsetOperationsList.get( 10 ).timeStamp(), equalTo( 1000L ) );
        assertThat( offsetOperationsList.get( 10 ).dependencyTimeStamp(), equalTo( 500L ) );
    }

    @Test
    public void shouldOffsetAndCompress()
    {
        // Given
        Iterator<Operation> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        // start times
                        gf.incrementing( 1000L, 100L ),
                        // dependency times
                        gf.incrementing( 900L, 50L ),
                        // names
                        gf.constant( "name1" )
                ),
                11
        );
        List<Operation> operationsList = ImmutableList.copyOf( operations );

        assertThat( operationsList.size(), is( 11 ) );
        assertThat( operationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 1000L ) );
        assertThat( operationsList.get( 0 ).timeStamp(), equalTo( 1000L ) );
        assertThat( operationsList.get( 0 ).dependencyTimeStamp(), equalTo( 900L ) );
        assertThat( operationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 1100L ) );
        assertThat( operationsList.get( 1 ).timeStamp(), equalTo( 1100L ) );
        assertThat( operationsList.get( 1 ).dependencyTimeStamp(), equalTo( 950L ) );
        assertThat( operationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 1200L ) );
        assertThat( operationsList.get( 2 ).timeStamp(), equalTo( 1200L ) );
        assertThat( operationsList.get( 2 ).dependencyTimeStamp(), equalTo( 1000L ) );
        assertThat( operationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 1300L ) );
        assertThat( operationsList.get( 3 ).timeStamp(), equalTo( 1300L ) );
        assertThat( operationsList.get( 3 ).dependencyTimeStamp(), equalTo( 1050L ) );
        assertThat( operationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 1400L ) );
        assertThat( operationsList.get( 4 ).timeStamp(), equalTo( 1400L ) );
        assertThat( operationsList.get( 4 ).dependencyTimeStamp(), equalTo( 1100L ) );
        assertThat( operationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 1500L ) );
        assertThat( operationsList.get( 5 ).timeStamp(), equalTo( 1500L ) );
        assertThat( operationsList.get( 5 ).dependencyTimeStamp(), equalTo( 1150L ) );
        assertThat( operationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 1600L ) );
        assertThat( operationsList.get( 6 ).timeStamp(), equalTo( 1600L ) );
        assertThat( operationsList.get( 6 ).dependencyTimeStamp(), equalTo( 1200L ) );
        assertThat( operationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 1700L ) );
        assertThat( operationsList.get( 7 ).timeStamp(), equalTo( 1700L ) );
        assertThat( operationsList.get( 7 ).dependencyTimeStamp(), equalTo( 1250L ) );
        assertThat( operationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 1800L ) );
        assertThat( operationsList.get( 8 ).timeStamp(), equalTo( 1800L ) );
        assertThat( operationsList.get( 8 ).dependencyTimeStamp(), equalTo( 1300L ) );
        assertThat( operationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 1900L ) );
        assertThat( operationsList.get( 9 ).timeStamp(), equalTo( 1900L ) );
        assertThat( operationsList.get( 9 ).dependencyTimeStamp(), equalTo( 1350L ) );
        assertThat( operationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 2000L ) );
        assertThat( operationsList.get( 10 ).timeStamp(), equalTo( 2000L ) );
        assertThat( operationsList.get( 10 ).dependencyTimeStamp(), equalTo( 1400L ) );

        // When
        long newStartTime = 500L;
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
        assertThat( offsetAndCompressedOperationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 500L ) );
        assertThat( offsetAndCompressedOperationsList.get( 0 ).timeStamp(), equalTo( 1000L ) );
        assertThat( offsetAndCompressedOperationsList.get( 0 ).dependencyTimeStamp(), equalTo( 900L ) );
        assertThat( offsetAndCompressedOperationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 520L ) );
        assertThat( offsetAndCompressedOperationsList.get( 1 ).timeStamp(), equalTo( 1100L ) );
        assertThat( offsetAndCompressedOperationsList.get( 1 ).dependencyTimeStamp(), equalTo( 950L ) );
        assertThat( offsetAndCompressedOperationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 540L ) );
        assertThat( offsetAndCompressedOperationsList.get( 2 ).timeStamp(), equalTo( 1200L ) );
        assertThat( offsetAndCompressedOperationsList.get( 2 ).dependencyTimeStamp(), equalTo( 1000L ) );
        assertThat( offsetAndCompressedOperationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 560L ) );
        assertThat( offsetAndCompressedOperationsList.get( 3 ).timeStamp(), equalTo( 1300L ) );
        assertThat( offsetAndCompressedOperationsList.get( 3 ).dependencyTimeStamp(), equalTo( 1050L ) );
        assertThat( offsetAndCompressedOperationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 580L ) );
        assertThat( offsetAndCompressedOperationsList.get( 4 ).timeStamp(), equalTo( 1400L ) );
        assertThat( offsetAndCompressedOperationsList.get( 4 ).dependencyTimeStamp(), equalTo( 1100L ) );
        assertThat( offsetAndCompressedOperationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 600L ) );
        assertThat( offsetAndCompressedOperationsList.get( 5 ).timeStamp(), equalTo( 1500L ) );
        assertThat( offsetAndCompressedOperationsList.get( 5 ).dependencyTimeStamp(), equalTo( 1150L ) );
        assertThat( offsetAndCompressedOperationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 620L ) );
        assertThat( offsetAndCompressedOperationsList.get( 6 ).timeStamp(), equalTo( 1600L ) );
        assertThat( offsetAndCompressedOperationsList.get( 6 ).dependencyTimeStamp(), equalTo( 1200L ) );
        assertThat( offsetAndCompressedOperationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 640L ) );
        assertThat( offsetAndCompressedOperationsList.get( 7 ).timeStamp(), equalTo( 1700L ) );
        assertThat( offsetAndCompressedOperationsList.get( 7 ).dependencyTimeStamp(), equalTo( 1250L ) );
        assertThat( offsetAndCompressedOperationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 660L ) );
        assertThat( offsetAndCompressedOperationsList.get( 8 ).timeStamp(), equalTo( 1800L ) );
        assertThat( offsetAndCompressedOperationsList.get( 8 ).dependencyTimeStamp(), equalTo( 1300L ) );
        assertThat( offsetAndCompressedOperationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 680L ) );
        assertThat( offsetAndCompressedOperationsList.get( 9 ).timeStamp(), equalTo( 1900L ) );
        assertThat( offsetAndCompressedOperationsList.get( 9 ).dependencyTimeStamp(), equalTo( 1350L ) );
        assertThat( offsetAndCompressedOperationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 700L ) );
        assertThat( offsetAndCompressedOperationsList.get( 10 ).timeStamp(), equalTo( 2000L ) );
        assertThat( offsetAndCompressedOperationsList.get( 10 ).dependencyTimeStamp(), equalTo( 1400L ) );
    }

    @Test
    public void shouldOffsetAndCompressWhenTimesAreVeryCloseTogetherWithoutRoundingErrors()
    {
        // Given
        Iterator<Operation> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        // start times
                        gf.incrementing( 0L, 1l ),
                        // dependency times
                        gf.incrementing( 0L, 0L ),
                        // names
                        gf.constant( "name1" )
                ),
                11
        );
        List<Operation> operationsList = ImmutableList.copyOf( operations );

        assertThat( operationsList.size(), is( 11 ) );
        assertThat( operationsList.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 0L ) );
        assertThat( operationsList.get( 0 ).timeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 0 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 1L ) );
        assertThat( operationsList.get( 1 ).timeStamp(), equalTo( 1L ) );
        assertThat( operationsList.get( 1 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 2L ) );
        assertThat( operationsList.get( 2 ).timeStamp(), equalTo( 2L ) );
        assertThat( operationsList.get( 2 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 3L ) );
        assertThat( operationsList.get( 3 ).timeStamp(), equalTo( 3L ) );
        assertThat( operationsList.get( 3 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 4L ) );
        assertThat( operationsList.get( 4 ).timeStamp(), equalTo( 4L ) );
        assertThat( operationsList.get( 4 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 5L ) );
        assertThat( operationsList.get( 5 ).timeStamp(), equalTo( 5L ) );
        assertThat( operationsList.get( 5 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 6L ) );
        assertThat( operationsList.get( 6 ).timeStamp(), equalTo( 6L ) );
        assertThat( operationsList.get( 6 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 7L ) );
        assertThat( operationsList.get( 7 ).timeStamp(), equalTo( 7L ) );
        assertThat( operationsList.get( 7 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 8L ) );
        assertThat( operationsList.get( 8 ).timeStamp(), equalTo( 8L ) );
        assertThat( operationsList.get( 8 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 9L ) );
        assertThat( operationsList.get( 9 ).timeStamp(), equalTo( 9L ) );
        assertThat( operationsList.get( 9 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( operationsList.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 10L ) );
        assertThat( operationsList.get( 10 ).timeStamp(), equalTo( 10L ) );
        assertThat( operationsList.get( 10 ).dependencyTimeStamp(), equalTo( 0L ) );

        // When
        long newStartTime = 0L;
        Double compressionRatio = 0.5;
        List<Operation> offsetAndCompressedOperations = ImmutableList
                .copyOf( gf.timeOffsetAndCompress( operationsList.iterator(), newStartTime, compressionRatio ) );

        // Then
        assertThat( offsetAndCompressedOperations.size(), is( 11 ) );
        assertThat( offsetAndCompressedOperations.get( 0 ).scheduledStartTimeAsMilli(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 0 ).timeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 0 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 1 ).scheduledStartTimeAsMilli(), equalTo( 1L ) );
        assertThat( offsetAndCompressedOperations.get( 1 ).timeStamp(), equalTo( 1L ) );
        assertThat( offsetAndCompressedOperations.get( 1 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 2 ).scheduledStartTimeAsMilli(), equalTo( 1L ) );
        assertThat( offsetAndCompressedOperations.get( 2 ).timeStamp(), equalTo( 2L ) );
        assertThat( offsetAndCompressedOperations.get( 2 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 3 ).scheduledStartTimeAsMilli(), equalTo( 2L ) );
        assertThat( offsetAndCompressedOperations.get( 3 ).timeStamp(), equalTo( 3L ) );
        assertThat( offsetAndCompressedOperations.get( 3 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 4 ).scheduledStartTimeAsMilli(), equalTo( 2L ) );
        assertThat( offsetAndCompressedOperations.get( 4 ).timeStamp(), equalTo( 4L ) );
        assertThat( offsetAndCompressedOperations.get( 4 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 5 ).scheduledStartTimeAsMilli(), equalTo( 3L ) );
        assertThat( offsetAndCompressedOperations.get( 5 ).timeStamp(), equalTo( 5L ) );
        assertThat( offsetAndCompressedOperations.get( 5 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 6 ).scheduledStartTimeAsMilli(), equalTo( 3L ) );
        assertThat( offsetAndCompressedOperations.get( 6 ).timeStamp(), equalTo( 6L ) );
        assertThat( offsetAndCompressedOperations.get( 6 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 7 ).scheduledStartTimeAsMilli(), equalTo( 4L ) );
        assertThat( offsetAndCompressedOperations.get( 7 ).timeStamp(), equalTo( 7L ) );
        assertThat( offsetAndCompressedOperations.get( 7 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 8 ).scheduledStartTimeAsMilli(), equalTo( 4L ) );
        assertThat( offsetAndCompressedOperations.get( 8 ).timeStamp(), equalTo( 8L ) );
        assertThat( offsetAndCompressedOperations.get( 8 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 9 ).scheduledStartTimeAsMilli(), equalTo( 5L ) );
        assertThat( offsetAndCompressedOperations.get( 9 ).timeStamp(), equalTo( 9L ) );
        assertThat( offsetAndCompressedOperations.get( 9 ).dependencyTimeStamp(), equalTo( 0L ) );
        assertThat( offsetAndCompressedOperations.get( 10 ).scheduledStartTimeAsMilli(), equalTo( 5L ) );
        assertThat( offsetAndCompressedOperations.get( 10 ).timeStamp(), equalTo( 10L ) );
        assertThat( offsetAndCompressedOperations.get( 10 ).dependencyTimeStamp(), equalTo( 0L ) );
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
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0L;
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
                new TimedNamedOperation2( 10L, 10L, 0L, "name2" ),
                new TimedNamedOperation2( 11L, 11L, 1L, "name2" ),
                new TimedNamedOperation1( 12L, 12L, 2L, "name1" )
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

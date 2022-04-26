package org.ldbcouncil.driver.client;

import org.ldbcouncil.driver.ClientException;
import org.ldbcouncil.driver.Workload;
import org.ldbcouncil.driver.WorkloadException;
import org.ldbcouncil.driver.WorkloadStreams;
import org.ldbcouncil.driver.control.ControlService;
import org.ldbcouncil.driver.control.LoggingService;
import org.ldbcouncil.driver.generator.GeneratorFactory;
import org.ldbcouncil.driver.generator.RandomDataGeneratorFactory;
import org.ldbcouncil.driver.runtime.metrics.MetricsCollectionException;
import org.ldbcouncil.driver.statistics.WorkloadStatistics;
import org.ldbcouncil.driver.statistics.WorkloadStatisticsCalculator;
import org.ldbcouncil.driver.util.Tuple3;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class CalculateWorkloadStatisticsMode implements ClientMode<WorkloadStatistics>
{
    private final ControlService controlService;
    private final LoggingService loggingService;
    private final long randomSeed;

    private Workload workload = null;
    private WorkloadStreams timeMappedWorkloadStreams = null;

    public CalculateWorkloadStatisticsMode( ControlService controlService, long randomSeed ) throws ClientException
    {
        this.controlService = controlService;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor( getClass().getSimpleName() );
        this.randomSeed = randomSeed;
    }

    @Override
    public void init() throws ClientException
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( randomSeed ) );
        WorkloadStreams workloadStreams;
        try
        {
            boolean returnStreamsWithDbConnector = false;
            Tuple3<WorkloadStreams,Workload,Long> workloadStreamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            controlService.configuration(),
                            gf,
                            returnStreamsWithDbConnector,
                            0,
                            controlService.configuration().operationCount(),
                            controlService.loggingServiceFactory()
                    );
            workloadStreams = workloadStreamsAndWorkload._1();
            workload = workloadStreamsAndWorkload._2();
        }
        catch ( Exception e )
        {
            throw new ClientException( format( "Error loading Workload class: %s",
                    controlService.configuration().workloadClassName() ), e );
        }
        loggingService.info( format( "Loaded Workload: %s", workload.getClass().getName() ) );

        loggingService.info(
                format( "Retrieving operation stream for workload: %s", workload.getClass().getSimpleName() ) );
        try
        {
            timeMappedWorkloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                    workloadStreams,
                    controlService.workloadStartTimeAsMilli(),
                    controlService.configuration().timeCompressionRatio(),
                    gf
            );
        }
        catch ( WorkloadException e )
        {
            throw new ClientException( "Error while retrieving operation stream for workload", e );
        }

        loggingService.info( "Driver Configuration" );
        loggingService.info( controlService.toString() );
    }

    @Override
    public WorkloadStatistics startExecutionAndAwaitCompletion() throws ClientException
    {
        loggingService.info(
                format( "Calculating workload statistics for: %s", workload.getClass().getSimpleName() ) );
        WorkloadStatistics workloadStatistics;
        try ( Workload w = workload )
        {
            WorkloadStatisticsCalculator workloadStatisticsCalculator = new WorkloadStatisticsCalculator();
            workloadStatistics = workloadStatisticsCalculator.calculate(
                    timeMappedWorkloadStreams,
                    TimeUnit.HOURS.toMillis( 5 )
                    // TODO uncomment, maybe
                    // workload.maxExpectedInterleave()
            );
            loggingService.info( "Calculation complete\n" + workloadStatistics );
        }
        catch ( MetricsCollectionException e )
        {
            throw new ClientException( "Error while calculating workload statistics", e );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Error while calculating workload statistics", e );
        }
        return null;
    }
}

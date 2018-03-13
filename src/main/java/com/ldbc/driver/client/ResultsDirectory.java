package com.ldbc.driver.client;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.util.FileUtils;
import com.ldbc.driver.util.MapUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.INITIALIZING;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.MEASUREMENT;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.MEASUREMENT_FINISHED;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.NOT_FOUND;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.WARMUP;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.WARMUP_FINISHED;
import static java.util.stream.Collectors.joining;

public class ResultsDirectory
{
    public enum BenchmarkPhase
    {
        NOT_FOUND,
        INITIALIZING,
        WARMUP,
        WARMUP_FINISHED,
        MEASUREMENT,
        MEASUREMENT_FINISHED
    }

    private static final String WARMUP_IDENTIFIER = "-WARMUP-";

    private static final String RESULTS_LOG_FILENAME_SUFFIX = "-results_log.csv";
    private static final String RESULTS_METRICS_FILENAME_SUFFIX = "-results.json";
    private static final String RESULTS_CONFIGURATION_FILENAME_SUFFIX = "-configuration.properties";

    private static final String RESULTS_VALIDATION_FILENAME_SUFFIX = "-validation.json";

    private final DriverConfiguration configuration;
    private final File resultsDir;

    public static ResultsDirectory fromDirectory( File resultsDir )
            throws IOException, DriverConfigurationException, ClientException
    {
        File configurationFile = findConfigurationFile( resultsDir, false );
        if ( null == configurationFile )
        {
            throw new RuntimeException( "Could not find configuration file in: " + resultsDir.getAbsolutePath() );
        }
        return new ResultsDirectory( getConfigurationFrom( configurationFile ) );
    }

    public ResultsDirectory( DriverConfiguration configuration ) throws ClientException
    {
        this.configuration = configuration;
        if ( null == configuration.resultDirPath() )
        {
            this.resultsDir = null;
        }
        else
        {
            this.resultsDir = new File( configuration.resultDirPath() );
            if ( this.resultsDir.exists() && !this.resultsDir.isDirectory() )
            {
                throw new ClientException( "Results directory is not directory: " + this.resultsDir.getAbsolutePath() );
            }
            else if ( !this.resultsDir.exists() )
            {
                try
                {
                    FileUtils.tryCreateDirs( this.resultsDir, false );
                }
                catch ( Exception e )
                {
                    throw new ClientException(
                            "Results directory could not be created: " + this.resultsDir.getAbsolutePath(), e );
                }
            }
        }
    }

    public boolean exists()
    {
        return null != resultsDir;
    }

    File getOrCreateResultsLogFile( boolean warmup ) throws ClientException
    {
        File resultsLog = getResultsLogFile( resultsDir, configuration, warmup );
        if ( !resultsLog.exists() )
        {
            try
            {
                FileUtils.createOrFail( resultsLog );
            }
            catch ( IOException e )
            {
                throw new ClientException( "Error creating results log file: " + resultsLog.getAbsolutePath(), e );
            }
        }
        return resultsLog;
    }

    public File getResultsLogFile( boolean warmup ) throws ClientException
    {
        return getResultsLogFile( resultsDir, configuration, warmup );
    }

    public long getResultsLogFileLength( boolean warmup ) throws ClientException
    {
        try ( SimpleCsvFileReader csvResultsLogReader = new SimpleCsvFileReader(
                getResultsLogFile( resultsDir, configuration, warmup ),
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING ) )
        {
            return Iterators.size( csvResultsLogReader );
        }
        catch ( FileNotFoundException e )
        {
            throw new ClientException(
                    "Error calculating length of " + getResultsLogFile( warmup ).getAbsolutePath(), e );
        }
    }

    public File getOrCreateResultsSummaryFile( boolean warmup ) throws ClientException
    {
        File resultsSummary = getResultsSummaryFile( warmup );
        if ( !resultsSummary.exists() )
        {
            try
            {
                FileUtils.createOrFail( resultsSummary );
            }
            catch ( IOException e )
            {
                throw new ClientException(
                        "Error creating results summary file: " + resultsSummary.getAbsolutePath(), e );
            }
        }
        return resultsSummary;
    }

    private File getResultsSummaryFile( boolean warmup ) throws ClientException
    {
        return getResultsSummaryFile( resultsDir, configuration, warmup );
    }

    File getOrCreateConfigurationFile( boolean warmup ) throws ClientException
    {
        File configurationFile = getConfigurationFile( warmup );
        if ( !configurationFile.exists() )
        {
            try
            {
                FileUtils.createOrFail( configurationFile );
            }
            catch ( IOException e )
            {
                throw new ClientException(
                        "Error creating configuration file: " + configurationFile.getAbsolutePath(), e );
            }
        }
        return configurationFile;
    }

    private File getConfigurationFile( boolean warmup ) throws ClientException
    {
        return new File( resultsDir, configurationFilename( configuration, warmup ) );
    }

    File getOrCreateResultsValidationFile( boolean warmup ) throws ClientException
    {
        File resultsValidationFile = getResultsValidationFile( warmup );
        if ( !resultsValidationFile.exists() )
        {
            try
            {
                FileUtils.createOrFail( resultsValidationFile );
            }
            catch ( IOException e )
            {
                throw new ClientException(
                        "Error creating results validation file: " + resultsValidationFile.getAbsolutePath(), e );
            }
        }
        return resultsValidationFile;
    }

    private File getResultsValidationFile( boolean warmup ) throws ClientException
    {
        return new File( resultsDir, resultsValidationFilename( configuration, warmup ) );
    }

    public Set<File> files() throws ClientException
    {
        return Sets.newHashSet( resultsDir.listFiles() );
    }

    public Set<File> expectedFiles() throws ClientException
    {
        if ( null == resultsDir )
        {
            return Collections.emptySet();
        }
        else
        {
            Set<File> expectedFiles = new HashSet<>();
            if ( configuration.warmupCount() > 0 )
            {
                if ( !configuration.ignoreScheduledStartTimes() )
                {
                    expectedFiles.add( getResultsValidationFile( true ) );
                }
                expectedFiles.add( getResultsLogFile( true ) );
                expectedFiles.add( getResultsSummaryFile( true ) );
                expectedFiles.add( getConfigurationFile( true ) );
            }
            if ( !configuration.ignoreScheduledStartTimes() )
            {
                expectedFiles.add( getResultsValidationFile( false ) );
            }
            expectedFiles.add( getResultsLogFile( false ) );
            expectedFiles.add( getResultsSummaryFile( false ) );
            expectedFiles.add( getConfigurationFile( false ) );
            return expectedFiles;
        }
    }

    public static BenchmarkPhase phase( File resultsDir )
            throws ClientException, DriverConfigurationException, IOException
    {
        if ( !exists( resultsDir ) )
        {
            // Results directory has not yet been created
            return NOT_FOUND;
        }

        File warmupResultsLog = findResultsLogFile( resultsDir, true );
        if ( null == warmupResultsLog || !warmupResultsLog.exists() )
        {
            // Warmup results log (the first file to be created during warmup) has not yet been created
            return INITIALIZING;
        }

        // Warmup results log exists. Warmup has started
        File warmupConfigurationFile = findConfigurationFile( resultsDir, true );
        if ( null == warmupConfigurationFile )
        {
            // Warmup results log is present, but warmup configuration file is not. Warmup is still running
            return WARMUP;
        }

        // Warmup configuration file exists
        DriverConfiguration warmupConfiguration = getConfigurationFrom( warmupConfigurationFile );
        File warmupSummary = getResultsSummaryFile( resultsDir, warmupConfiguration, true );
        if ( !warmupSummary.exists() )
        {
            // Warmup configuration file is present, but warmup summary file is not. Warmup is still running
            return WARMUP;
        }

        // Warmup summary exists. Warmup as finished
        File measurementResultsLogFile = findResultsLogFile( resultsDir, false );
        if ( null == measurementResultsLogFile || !measurementResultsLogFile.exists() )
        {
            // Warmup summary is present, but measurement results log is not. Waiting for measurement to start
            return WARMUP_FINISHED;
        }

        // Measurement results log exists. Measurement has started
        File measurementSummary = getResultsSummaryFile( resultsDir, warmupConfiguration, false );
        if ( !measurementSummary.exists() )
        {
            // Measurement results log is present, but measurement summary file is not. Measurement is still running
            return MEASUREMENT;
        }

        // Measurement summary file is present. We're done
        return MEASUREMENT_FINISHED;
    }

    private static boolean exists( File resultsDir )
    {
        return resultsDir != null && resultsDir.isDirectory() && resultsDir.exists();
    }

    private static File findConfigurationFile( File resultsDir, boolean warmup )
            throws DriverConfigurationException, IOException
    {
        FileFilter configurationFileFilter = file ->
                file.getName().contains( WARMUP_IDENTIFIER ) == warmup &&
                file.getName().endsWith( RESULTS_CONFIGURATION_FILENAME_SUFFIX );
        File[] resultFiles = resultsDir.listFiles( configurationFileFilter );
        if ( null == resultFiles || resultFiles.length != 1 )
        {
            return null;
        }
        else
        {
            return resultFiles[0];
        }
    }

    private static DriverConfiguration getConfigurationFrom( File configurationFile )
            throws DriverConfigurationException, IOException
    {
        Map<String,String> configurationMap = MapUtils.loadPropertiesToMap( configurationFile );
        return ConsoleAndFileDriverConfiguration.fromParamsMap( configurationMap );
    }

    private static File findResultsLogFile( File resultsDir, boolean warmup )
            throws DriverConfigurationException, IOException
    {
        FileFilter resultsLogFileFilter = file ->
                file.getName().contains( WARMUP_IDENTIFIER ) == warmup &&
                file.getName().endsWith( RESULTS_LOG_FILENAME_SUFFIX );
        File[] resultFiles = resultsDir.listFiles( resultsLogFileFilter );
        if ( null == resultFiles || resultFiles.length == 0 )
        {
            return null;
        }
        else if ( resultFiles.length > 1 )
        {
            throw new RuntimeException(
                    "Expected to find 1 results log file, but found: " +
                    Arrays.stream( resultFiles ).map( File::getAbsolutePath ).collect( joining( "," ) ) );
        }
        else
        {
            return resultFiles[0];
        }
    }

    private static File getResultsLogFile( File resultsDir, DriverConfiguration configuration, boolean warmup )
            throws ClientException
    {
        return new File( resultsDir, resultsLogFilename( configuration, warmup ) );
    }

    private static File getResultsSummaryFile( File resultsDir, DriverConfiguration configuration, boolean warmup )
            throws ClientException
    {
        return new File( resultsDir, resultsSummaryFilename( configuration, warmup ) );
    }

    private static String resultsValidationFilename( DriverConfiguration configuration, boolean warmup )
    {
        return (warmup) ? configuration.name() + WARMUP_IDENTIFIER + RESULTS_VALIDATION_FILENAME_SUFFIX
                        : configuration.name() + RESULTS_VALIDATION_FILENAME_SUFFIX;
    }

    private static String resultsLogFilename( DriverConfiguration configuration, boolean warmup )
    {
        return (warmup) ? configuration.name() + WARMUP_IDENTIFIER + RESULTS_LOG_FILENAME_SUFFIX
                        : configuration.name() + RESULTS_LOG_FILENAME_SUFFIX;
    }

    private static String resultsSummaryFilename( DriverConfiguration configuration, boolean warmup )
    {
        return (warmup) ? configuration.name() + WARMUP_IDENTIFIER + RESULTS_METRICS_FILENAME_SUFFIX
                        : configuration.name() + RESULTS_METRICS_FILENAME_SUFFIX;
    }

    private static String configurationFilename( DriverConfiguration configuration, boolean warmup )
    {
        return (warmup) ? configuration.name() + WARMUP_IDENTIFIER + RESULTS_CONFIGURATION_FILENAME_SUFFIX
                        : configuration.name() + RESULTS_CONFIGURATION_FILENAME_SUFFIX;
    }
}

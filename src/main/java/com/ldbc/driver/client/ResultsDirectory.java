package com.ldbc.driver.client;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

public class ResultsDirectory
{
    private static final String WARMUP_IDENTIFIER = "-WARMUP-";

    public static final String RESULTS_LOG_FILENAME_SUFFIX = "-results_log.csv";
    public static final String RESULTS_METRICS_FILENAME_SUFFIX = "-results.json";
    public static final String RESULTS_CONFIGURATION_FILENAME_SUFFIX = "-configuration.properties";

    public static final String RESULTS_VALIDATION_FILENAME_SUFFIX = "-validation.json";

    private final DriverConfiguration configuration;
    private final File resultsDir;

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
            if ( this.resultsDir.exists() && !resultsDir.isDirectory() )
            {
                throw new ClientException( "Results directory is not directory: " + this.resultsDir.getAbsolutePath() );
            }
            else if ( !this.resultsDir.exists() )
            {
                try
                {
                    FileUtils.forceMkdir( this.resultsDir );
                }
                catch ( IOException e )
                {
                    throw new ClientException(
                            format( "Results directory does not exist and could not be created: %s",
                                    this.resultsDir.getAbsolutePath() ),
                            e
                    );
                }
            }
        }
    }

    public boolean exists()
    {
        return null != resultsDir;
    }

    public File getOrCreateResultsLogFile( boolean warmup ) throws ClientException
    {
        File resultsLog = getResultsLogFile( warmup );
        if ( !resultsLog.exists() )
        {
            try
            {
                com.ldbc.driver.util.FileUtils.createOrFail( resultsLog );
            }
            catch ( IOException e )
            {
                throw new ClientException(
                        format( "Error creating results log file: %s", resultsLog.getAbsolutePath() ), e
                );
            }
        }
        return resultsLog;
    }

    public File getResultsLogFile( boolean warmup ) throws ClientException
    {
        if ( null == resultsDir )
        {
            throw new ClientException( "Results directory is null" );
        }
        else
        {
            return new File( resultsDir, resultsLogFilename( warmup ) );
        }
    }

    public long getResultsLogFileLength( boolean warmup ) throws ClientException
    {
        try ( SimpleCsvFileReader csvResultsLogReader = new SimpleCsvFileReader(
                getResultsLogFile( warmup ),
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING
        ) )
        {
            return Iterators.size( csvResultsLogReader );
        }
        catch ( FileNotFoundException e )
        {
            throw new ClientException(
                    format( "Error calculating length of %s", getResultsLogFile( warmup ).getAbsolutePath() ), e
            );
        }
    }

    public File getOrCreateResultsSummaryFile( boolean warmup ) throws ClientException
    {
        File resultsSummary = getResultsSummaryFile( warmup );
        if ( !resultsSummary.exists() )
        {
            try
            {
                com.ldbc.driver.util.FileUtils.createOrFail( resultsSummary );
            }
            catch ( IOException e )
            {
                throw new ClientException(
                        format( "Error creating results summary file: %s", resultsSummary.getAbsolutePath() ), e
                );
            }
        }
        return resultsSummary;
    }

    public File getResultsSummaryFile( boolean warmup ) throws ClientException
    {
        if ( null == resultsDir )
        {
            throw new ClientException( "Results directory is null" );
        }
        else
        {
            return new File( resultsDir, resultsSummaryFilename( warmup ) );
        }
    }

    public File getOrCreateConfigurationFile( boolean warmup ) throws ClientException
    {
        File configurationFile = getConfigurationFile( warmup );
        if ( !configurationFile.exists() )
        {
            try
            {
                com.ldbc.driver.util.FileUtils.createOrFail( configurationFile );
            }
            catch ( IOException e )
            {
                throw new ClientException(
                        format( "Error creating configuration file: %s", configurationFile.getAbsolutePath() ), e
                );
            }
        }
        return configurationFile;
    }

    public File getConfigurationFile( boolean warmup ) throws ClientException
    {
        if ( null == resultsDir )
        {
            throw new ClientException( "Results directory is null" );
        }
        else
        {
            return new File( resultsDir, configurationFilename( warmup ) );
        }
    }

    public File getOrCreateResultsValidationFile( boolean warmup ) throws ClientException
    {
        File resultsValidationFile = getResultsValidationFile( warmup );
        if ( !resultsValidationFile.exists() )
        {
            try
            {
                com.ldbc.driver.util.FileUtils.createOrFail( resultsValidationFile );
            }
            catch ( IOException e )
            {
                throw new ClientException(
                        format( "Error creating results validation file: %s", resultsValidationFile.getAbsolutePath() ),
                        e
                );
            }
        }
        return resultsValidationFile;
    }

    public File getResultsValidationFile( boolean warmup ) throws ClientException
    {
        if ( null == resultsDir )
        {
            throw new ClientException( "Results directory is null" );
        }
        else
        {
            return new File( resultsDir, resultsValidationFilename( warmup ) );
        }
    }

    public Set<File> files() throws ClientException
    {
        if ( null == resultsDir )
        {
            throw new ClientException( "Results directory is null" );
        }
        else
        {
            return Sets.newHashSet( resultsDir.listFiles() );
        }
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

    private String resultsValidationFilename( boolean warmup )
    {
        return (warmup) ? configuration.name() + WARMUP_IDENTIFIER + RESULTS_VALIDATION_FILENAME_SUFFIX
                        : configuration.name() + RESULTS_VALIDATION_FILENAME_SUFFIX;
    }

    private String resultsLogFilename( boolean warmup )
    {
        return (warmup) ? configuration.name() + WARMUP_IDENTIFIER +
                          RESULTS_LOG_FILENAME_SUFFIX
                        : configuration.name() + RESULTS_LOG_FILENAME_SUFFIX;
    }

    private String resultsSummaryFilename( boolean warmup )
    {
        return (warmup) ? configuration.name() + WARMUP_IDENTIFIER +
                          RESULTS_METRICS_FILENAME_SUFFIX
                        : configuration.name() + RESULTS_METRICS_FILENAME_SUFFIX;
    }

    private String configurationFilename( boolean warmup )
    {
        return (warmup) ? configuration.name() + WARMUP_IDENTIFIER +
                          RESULTS_CONFIGURATION_FILENAME_SUFFIX
                        : configuration.name() + RESULTS_CONFIGURATION_FILENAME_SUFFIX;
    }
}

package org.ldbcouncil.snb.driver.control;
/**
 * Interface defining parameters required to run the driver
 */

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface DriverConfiguration
{
    // General
    String name();

    String dbClassName();

    String workloadClassName();

    long operationCount();

    int threadCount();

    int statusDisplayIntervalAsSeconds();

    TimeUnit timeUnit();

    String resultDirPath();

    double timeCompressionRatio();

    boolean validationCreationParams();

    int validationParametersSize();

    boolean validationSerializationCheck();

    String databaseValidationFilePath();

    boolean calculateWorkloadStatistics();

    long spinnerSleepDurationAsMilli();

    boolean shouldPrintHelpString();

    String helpString();

    boolean ignoreScheduledStartTimes();

    long warmupCount();

    long skipCount();

    boolean flushLog();

    String toPropertiesString() throws DriverConfigurationException;

    Map<String,String> asMap();

    DriverConfiguration applyArgs( DriverConfiguration newConfiguration ) throws DriverConfigurationException;

    DriverConfiguration applyArg( String argument, String newValue ) throws DriverConfigurationException;

    DriverConfiguration applyArgs( Map<String,String> newMap ) throws DriverConfigurationException;
}

package com.ldbc.driver.control;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface DriverConfiguration
{
    String name();

    String dbClassName();

    String workloadClassName();

    long operationCount();

    int threadCount();

    int statusDisplayIntervalAsSeconds();

    TimeUnit timeUnit();

    String resultDirPath();

    double timeCompressionRatio();

    Set<String> peerIds();

    ValidationParamOptions validationParamsCreationOptions();

    String databaseValidationFilePath();

    boolean calculateWorkloadStatistics();

    long spinnerSleepDurationAsMilli();

    boolean shouldPrintHelpString();

    String helpString();

    boolean ignoreScheduledStartTimes();

    long warmupCount();

    long skipCount();

    String toPropertiesString() throws DriverConfigurationException;

    Map<String,String> asMap();

    DriverConfiguration applyArgs( DriverConfiguration newConfiguration ) throws DriverConfigurationException;

    DriverConfiguration applyArg( String argument, String newValue ) throws DriverConfigurationException;

    DriverConfiguration applyArgs( Map<String,String> newMap ) throws DriverConfigurationException;

    public interface ValidationParamOptions
    {
        public String filePath();

        public int validationSetSize();
    }
}

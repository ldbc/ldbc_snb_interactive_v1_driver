package com.ldbc.driver.control;

import com.ldbc.driver.modes.DriverModeType;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface DriverConfiguration
{

    DriverModeType getDriverMode();

    String getName();

    String getDbClassName();

    String getWorkloadClassName();

    long getOperationCount();

    int getThreadCount();

    int statusDisplayIntervalAsSeconds();

    TimeUnit timeUnit();

    String resultDirPath();

    double timeCompressionRatio();

    ValidationParamOptions getValidationParamsCreationOptions();

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

    interface ValidationParamOptions
    {
        String getFilePath();

        int getValidationSetSize();
    }
}

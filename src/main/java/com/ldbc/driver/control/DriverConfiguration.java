package com.ldbc.driver.control;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface DriverConfiguration {
    String name();

    String dbClassName();

    String workloadClassName();

    long operationCount();

    int threadCount();

    int statusDisplayIntervalAsSeconds();

    TimeUnit timeUnit();

    String resultDirPath();

    double timeCompressionRatio();

    long windowedExecutionWindowDurationAsMilli();

    Set<String> peerIds();

    long toleratedExecutionDelayAsMilli();

    ValidationParamOptions validationParamsCreationOptions();

    String databaseValidationFilePath();

    boolean validateWorkload();

    boolean calculateWorkloadStatistics();

    long spinnerSleepDurationAsMilli();

    boolean shouldPrintHelpString();

    String helpString();

    boolean ignoreScheduledStartTimes();

    boolean shouldCreateResultsLog();

    String toPropertiesString() throws DriverConfigurationException;

    Map<String, String> asMap();

    DriverConfiguration applyMap(Map<String, String> newMap) throws DriverConfigurationException;

    public interface ValidationParamOptions {
        public String filePath();

        public int validationSetSize();
    }
}

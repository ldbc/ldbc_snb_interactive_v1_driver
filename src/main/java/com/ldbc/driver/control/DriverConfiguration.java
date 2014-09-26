package com.ldbc.driver.control;

import com.ldbc.driver.temporal.Duration;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface DriverConfiguration {
    String name();

    String dbClassName();

    String workloadClassName();

    long operationCount();

    int threadCount();

    Duration statusDisplayInterval();

    TimeUnit timeUnit();

    String resultDirPath();

    double timeCompressionRatio();

    Duration windowedExecutionWindowDuration();

    Set<String> peerIds();

    Duration toleratedExecutionDelay();

    ValidationParamOptions validationParamsCreationOptions();

    String databaseValidationFilePath();

    boolean validateWorkload();

    boolean calculateWorkloadStatistics();

    Duration spinnerSleepDuration();

    boolean shouldPrintHelpString();

    String helpString();

    String toPropertiesString() throws DriverConfigurationException;

    Map<String, String> asMap();

    DriverConfiguration applyMap(Map<String, String> newMap) throws DriverConfigurationException;

    public interface ValidationParamOptions {
        public String filePath();

        public int validationSetSize();
    }
}

package com.ldbc.driver.control;

import com.ldbc.driver.temporal.Duration;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface DriverConfiguration {
    String dbClassName();

    String workloadClassName();

    long operationCount();

    int threadCount();

    boolean showStatus();

    TimeUnit timeUnit();

    String resultFilePath();

    double timeCompressionRatio();

    Duration gctDeltaDuration();

    Duration compressedGctDeltaDuration();

    Set<String> peerIds();

    Duration toleratedExecutionDelay();

    ValidationParamOptions validationCreationParams();

    String databaseValidationFilePath();

    boolean validateWorkload();

    boolean calculateWorkloadStatistics();

    Duration spinnerSleepDuration();

    Map<String, String> asMap();

    DriverConfiguration applyMap(Map<String, String> newMap) throws DriverConfigurationException;

    public interface ValidationParamOptions {
        public String filePath();

        public int validationSetSize();
    }
}

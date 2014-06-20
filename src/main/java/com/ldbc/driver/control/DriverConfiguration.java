package com.ldbc.driver.control;

import com.ldbc.driver.temporal.Duration;

import java.util.List;
import java.util.Map;
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

    List<String> peerIds();

    Duration toleratedExecutionDelay();

    boolean validateDatabase();

    boolean validateWorkload();

    boolean calculateWorkloadStatistics();

    Duration spinnerSleepDuration();

    Map<String, String> asMap();

    DriverConfiguration applyMap(Map<String, String> newMap) throws DriverConfigurationException;
}

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

    boolean isShowStatus();

    TimeUnit timeUnit();

    String resultFilePath();

    Double timeCompressionRatio();

    Duration gctDeltaDuration();

    List<String> peerIds();

    Duration toleratedExecutionDelay();

    public Map<String, String> asMap();
}

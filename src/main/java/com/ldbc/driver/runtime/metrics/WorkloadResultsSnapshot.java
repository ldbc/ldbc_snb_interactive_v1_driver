package com.ldbc.driver.runtime.metrics;

import com.google.common.collect.Lists;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WorkloadResultsSnapshot {
    @JsonProperty(value = "all_metrics")
    private List<OperationMetricsSnapshot> metrics;

    @JsonProperty(value = "unit")
    private TimeUnit unit;

    @JsonProperty(value = "start_time")
    private long startTimeAsUnit;

    @JsonProperty(value = "finish_time")
    private long finishTimeAsUnit;

    @JsonProperty(value = "total_duration")
    private long totalRunDurationAsUnit;

    @JsonProperty(value = "total_count")
    private long operationCount;

    public static WorkloadResultsSnapshot fromJson(File jsonFile) throws
            IOException {
        return new ObjectMapper().readValue(jsonFile, WorkloadResultsSnapshot.class);
    }

    public static WorkloadResultsSnapshot fromJson(String jsonString) throws IOException {
        return new ObjectMapper().readValue(jsonString, WorkloadResultsSnapshot.class);
    }

    private WorkloadResultsSnapshot() {
    }

    public WorkloadResultsSnapshot(Map<String, OperationMetricsSnapshot> metrics, Time startTime, Time finishTime, long operationCount, TimeUnit unit) {
        this.metrics = Lists.newArrayList(metrics.values());
        Collections.sort(this.metrics, new OperationMetricsManager.OperationMetricsNameComparator());
        this.startTimeAsUnit = startTime.as(unit);
        this.finishTimeAsUnit = finishTime.as(unit);
        this.totalRunDurationAsUnit = finishTime.greaterBy(startTime).as(unit);
        this.operationCount = operationCount;
        this.unit = unit;
    }

    @JsonProperty(value = "all_metrics")
    public List<OperationMetricsSnapshot> allMetrics() {
        return metrics;
    }

    @JsonProperty(value = "all_metrics")
    private void setAllMetrics(List<OperationMetricsSnapshot> metrics) {
        this.metrics = metrics;
        Collections.sort(metrics, new OperationMetricsManager.OperationMetricsNameComparator());
    }

    public Time startTime() {
        return Time.from(unit, startTimeAsUnit);
    }

    public Time finishTime() {
        return Time.from(unit, finishTimeAsUnit);
    }

    public Duration totalRunDuration() {
        return Duration.from(unit, totalRunDurationAsUnit);
    }

    public long totalOperationCount() {
        return operationCount;
    }

    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            System.out.println(ConcurrentErrorReporter.stackTraceToString(e));
            throw new RuntimeException("Unable to generate parameter values string", e.getCause());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WorkloadResultsSnapshot that = (WorkloadResultsSnapshot) o;

        if (finishTimeAsUnit != that.finishTimeAsUnit) return false;
        if (operationCount != that.operationCount) return false;
        if (startTimeAsUnit != that.startTimeAsUnit) return false;
        if (totalRunDurationAsUnit != that.totalRunDurationAsUnit) return false;
        if (metrics != null ? !metrics.equals(that.metrics) : that.metrics != null) return false;
        if (unit != that.unit) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = metrics != null ? metrics.hashCode() : 0;
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        result = 31 * result + (int) (startTimeAsUnit ^ (startTimeAsUnit >>> 32));
        result = 31 * result + (int) (finishTimeAsUnit ^ (finishTimeAsUnit >>> 32));
        result = 31 * result + (int) (totalRunDurationAsUnit ^ (totalRunDurationAsUnit >>> 32));
        result = 31 * result + (int) (operationCount ^ (operationCount >>> 32));
        return result;
    }
}
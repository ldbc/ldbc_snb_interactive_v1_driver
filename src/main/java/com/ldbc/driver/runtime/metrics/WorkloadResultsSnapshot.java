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

    @JsonProperty(value = "format_version")
    private int formatVersion = 1;

    @JsonProperty(value = "unit")
    private TimeUnit unit;

    @JsonProperty(value = "start_time")
    private long startTimeAsUnit;

    @JsonProperty(value = "latest_finish_time")
    private long latestFinishTimeAsUnit;

    @JsonProperty(value = "total_duration")
    private long totalRunDurationAsUnit;

    @JsonProperty(value = "total_count")
    private long operationCount;

    // TODO test
    public static WorkloadResultsSnapshot fromJson(File jsonFile) throws
            IOException {
        return new ObjectMapper().readValue(jsonFile, WorkloadResultsSnapshot.class);
    }

    public static WorkloadResultsSnapshot fromJson(String jsonString) throws IOException {
        return new ObjectMapper().readValue(jsonString, WorkloadResultsSnapshot.class);
    }

    private WorkloadResultsSnapshot() {
    }

    public WorkloadResultsSnapshot(Map<String, OperationMetricsSnapshot> metrics, Time startTime, Time latestFinishTime, long operationCount, TimeUnit unit) {
        this.metrics = Lists.newArrayList(metrics.values());
        Collections.sort(this.metrics, new OperationMetricsManager.OperationMetricsNameComparator());
        this.startTimeAsUnit = startTime.as(unit);
        this.latestFinishTimeAsUnit = latestFinishTime.as(unit);
        this.totalRunDurationAsUnit = latestFinishTime.durationGreaterThan(startTime).as(unit);
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

    public Time latestFinishTime() {
        return Time.from(unit, latestFinishTimeAsUnit);
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
            throw new RuntimeException("Unable to generate parameter values string", e);
        }
    }

    @Override
    public String toString() {
        return "WorkloadResultsSnapshot{" +
                "metrics=" + metrics +
                ", unit=" + unit +
                ", startTimeAsUnit=" + startTimeAsUnit +
                ", latestFinishTimeAsUnit=" + latestFinishTimeAsUnit +
                ", totalRunDurationAsUnit=" + totalRunDurationAsUnit +
                ", operationCount=" + operationCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WorkloadResultsSnapshot that = (WorkloadResultsSnapshot) o;

        if (latestFinishTimeAsUnit != that.latestFinishTimeAsUnit) return false;
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
        result = 31 * result + (int) (latestFinishTimeAsUnit ^ (latestFinishTimeAsUnit >>> 32));
        result = 31 * result + (int) (totalRunDurationAsUnit ^ (totalRunDurationAsUnit >>> 32));
        result = 31 * result + (int) (operationCount ^ (operationCount >>> 32));
        return result;
    }
}
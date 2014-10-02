package com.ldbc.driver.runtime.metrics;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleOperationMetricsFormatter implements OperationMetricsFormatter {
    private static final String DEFAULT_NAME = "<no name given>";
    private static final String DEFAULT_UNIT = "<no unit given>";
    private static final String OFFSET = "    ";

    public String format(WorkloadResultsSnapshot workloadResultsSnapshot) {
        List<OperationMetricsSnapshot> sortedMetrics = Lists.newArrayList(workloadResultsSnapshot.allMetrics());
        Collections.sort(sortedMetrics, new OperationTypeMetricsManager.OperationMetricsNameComparator());

        StringBuilder sb = new StringBuilder();
        sb.append("Runtime\n");
        for (OperationMetricsSnapshot metric : sortedMetrics) {
            sb.append(formatOneMetricRuntime(OFFSET, metric));
        }
        sb.append("Start Time Delay\n");
        for (OperationMetricsSnapshot metric : sortedMetrics) {
            sb.append(formatOneMetricStartTimeDelay(OFFSET, metric));
        }
        sb.append("Result\n");
        for (OperationMetricsSnapshot metric : sortedMetrics) {
            sb.append(formatOneMetricResult(OFFSET, metric));
        }
        return sb.toString();
    }

    private String formatOneMetricRuntime(String offset, OperationMetricsSnapshot metric) {
        int padRightDistance = 20;
        String name = (null == metric.name()) ? DEFAULT_NAME : metric.name();
        String unit = (null == metric.durationUnit()) ? DEFAULT_UNIT : metric.durationUnit().toString();
        StringBuilder sb = new StringBuilder();
        sb.append(offset).append(String.format("%s\n", name));
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Units:")).append(unit).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Count:")).append(metric.runTimeMetric().count()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Min:")).append(metric.runTimeMetric().min()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Max:")).append(metric.runTimeMetric().max()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Mean:")).append(metric.runTimeMetric().mean()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "50th Percentile:")).append(metric.runTimeMetric().percentile50()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "90th Percentile:")).append(metric.runTimeMetric().percentile90()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "95th Percentile:")).append(metric.runTimeMetric().percentile95()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "99th Percentile:")).append(metric.runTimeMetric().percentile99()).append("\n");
        return sb.toString();
    }

    private String formatOneMetricStartTimeDelay(String offset, OperationMetricsSnapshot metric) {
        int padRightDistance = 20;
        String name = (null == metric.name()) ? DEFAULT_NAME : metric.name();
        String unit = (null == metric.durationUnit()) ? DEFAULT_UNIT : metric.durationUnit().toString();
        StringBuilder sb = new StringBuilder();
        sb.append(offset).append(String.format("%s\n", name));
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Units:")).append(unit).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Count:")).append(metric.startTimeDelayMetric().count()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Min:")).append(metric.startTimeDelayMetric().min()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Max:")).append(metric.startTimeDelayMetric().max()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Mean:")).append(metric.startTimeDelayMetric().mean()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "50th Percentile:")).append(metric.startTimeDelayMetric().percentile50()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "90th Percentile:")).append(metric.startTimeDelayMetric().percentile90()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "95th Percentile:")).append(metric.startTimeDelayMetric().percentile95()).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "99th Percentile:")).append(metric.startTimeDelayMetric().percentile99()).append("\n");
        return sb.toString();
    }

    private String formatOneMetricResult(String offset, OperationMetricsSnapshot metric) {
        int padRightDistance = 20;
        int padRightDistanceForResultCodes = 10;
        StringBuilder sb = new StringBuilder();
        String name = (null == metric.name()) ? DEFAULT_NAME : metric.name();
        String unit = (null == metric.durationUnit()) ? DEFAULT_UNIT : metric.durationUnit().toString();
        sb.append(offset).append(String.format("%s\n", name));
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Units:")).append(unit).append("\n");
        sb.append(offset).append(offset).append(String.format("%1$-" + padRightDistance + "s", "Count:")).append(metric.resultCodeMetric().count()).append("\n");
        sb.append(offset).append(offset).append(String.format("Values:\n"));
        for (Map.Entry<Long, Long> measurement : metric.resultCodeMetric().allValues().entrySet()) {
            sb.append(offset).append(offset).append(offset).append(String.format("%1$-" + padRightDistanceForResultCodes + "s", measurement.getKey() + ":")).append(measurement.getValue()).append("\n");
        }
        return sb.toString();
    }
}

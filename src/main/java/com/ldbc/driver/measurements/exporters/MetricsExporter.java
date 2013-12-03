package com.ldbc.driver.measurements.exporters;

import com.ldbc.driver.measurements.MetricGroup;
import com.ldbc.driver.measurements.MetricsExporterException;
import com.ldbc.driver.measurements.formatters.MetricFormatter;
import com.ldbc.driver.measurements.metric.Metric;

public interface MetricsExporter
{
    public <M extends Metric> void export( MetricFormatter<M> metricsFormatter, MetricGroup<M>... metricGroups )
            throws MetricsExporterException;

    public <M extends Metric> void export( MetricFormatter<M> metricsFormatter, M... metrics )
            throws MetricsExporterException;
}

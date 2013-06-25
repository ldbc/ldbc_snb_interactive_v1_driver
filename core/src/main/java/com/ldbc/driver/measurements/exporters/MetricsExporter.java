package com.ldbc.driver.measurements.exporters;

import com.ldbc.driver.measurements.Metric;
import com.ldbc.driver.measurements.MetricGroup;
import com.ldbc.driver.measurements.MetricsExporterException;
import com.ldbc.driver.measurements.formatters.MetricsFormatter;

public interface MetricsExporter
{
    public void export( MetricsFormatter metricsFormatter, MetricGroup... metricGroups )
            throws MetricsExporterException;

    public void export( MetricsFormatter metricsFormatter, Metric... metrics ) throws MetricsExporterException;
}

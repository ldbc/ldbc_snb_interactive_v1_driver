package com.ldbc.driver.measurements.exporters;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import com.ldbc.driver.measurements.MetricGroup;
import com.ldbc.driver.measurements.MetricsExporterException;
import com.ldbc.driver.measurements.formatters.MetricFormatter;
import com.ldbc.driver.measurements.metric.Metric;

public class OutputStreamMetricsExporter implements MetricsExporter
{
    public static final Charset DEFAULT_CHARSET = Charset.forName( "UTF-8" );

    private final OutputStream outputStream;
    private final Charset charset;

    public OutputStreamMetricsExporter( OutputStream outputStream )
    {
        this( outputStream, DEFAULT_CHARSET );
    }

    public OutputStreamMetricsExporter( OutputStream outputStream, Charset charset )
    {
        this.outputStream = outputStream;
        this.charset = charset;
    }

    @Override
    public <M extends Metric> void export( MetricFormatter<M> metricsFormatter, MetricGroup<M>... metricGroups )
            throws MetricsExporterException
    {
        try
        {
            String formattedMetricsGroups = metricsFormatter.format( metricGroups );
            outputStream.write( formattedMetricsGroups.getBytes( charset ) );
        }
        catch ( IOException e )
        {
            String errMsg = "Error encountered while writing to output stream";
            throw new MetricsExporterException( errMsg, e.getCause() );
        }
    }

    @Override
    public <M extends Metric> void export( MetricFormatter<M> metricsFormatter, M... metrics )
            throws MetricsExporterException
    {
        try
        {
            String formattedMetrics = metricsFormatter.format( metrics );
            outputStream.write( formattedMetrics.getBytes( charset ) );
        }
        catch ( IOException e )
        {
            String errMsg = "Error encountered while writing to output stream";
            throw new MetricsExporterException( errMsg, e.getCause() );
        }
    }
}

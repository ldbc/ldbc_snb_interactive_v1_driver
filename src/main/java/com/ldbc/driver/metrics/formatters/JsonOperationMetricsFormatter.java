package com.ldbc.driver.metrics.formatters;

import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Lists;
import com.ldbc.driver.metrics.OperationMetrics;
import com.ldbc.driver.metrics.OperationMetrics.OperationMetricsNameComparator;

public class JsonOperationMetricsFormatter implements OperationMetricsFormatter
{
    public String format( Iterable<OperationMetrics> metrics )
    {
        List<OperationMetrics> sortedMetrics = Lists.newArrayList( metrics );
        Collections.sort( sortedMetrics, new OperationMetricsNameComparator() );
        try
        {
            return new ObjectMapper().writeValueAsString( sortedMetrics );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( "Error encountered serializing metrics to JSON", e.getCause() );
        }
    }
}

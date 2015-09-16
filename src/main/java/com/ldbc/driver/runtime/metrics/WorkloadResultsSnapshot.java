package com.ldbc.driver.runtime.metrics;

import com.google.common.collect.Lists;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WorkloadResultsSnapshot
{
    @JsonProperty( value = "all_metrics" )
    private List<OperationMetricsSnapshot> metrics;

    @JsonProperty( value = "format_version" )
    private int formatVersion = 3;

    @JsonProperty( value = "unit" )
    private TimeUnit unit;

    @JsonProperty( value = "start_time" )
    private long startTimeAsUnit;

    @JsonProperty( value = "latest_finish_time" )
    private long latestFinishTimeAsUnit;

    @JsonProperty( value = "total_duration" )
    private long totalRunDurationAsUnit;

    @JsonProperty( value = "total_count" )
    private long operationCount;

    @JsonProperty( value = "throughput" )
    private double throughput;

    public static WorkloadResultsSnapshot fromJson( File jsonFile ) throws IOException
    {
        return new ObjectMapper().readValue( jsonFile, WorkloadResultsSnapshot.class );
    }

    public static WorkloadResultsSnapshot fromJson( String jsonString ) throws IOException
    {
        return new ObjectMapper().readValue( jsonString, WorkloadResultsSnapshot.class );
    }

    private WorkloadResultsSnapshot()
    {
    }

    public WorkloadResultsSnapshot( Map<String,OperationMetricsSnapshot> metrics,
            long startTimeAsMilli,
            long latestFinishTimeAsMilli,
            long operationCount,
            TimeUnit unit )
    {
        this.metrics = Lists.newArrayList( metrics.values() );
        Collections.sort( this.metrics, new OperationTypeMetricsManager.OperationMetricsNameComparator() );
        this.startTimeAsUnit = unit.convert( startTimeAsMilli, TimeUnit.MILLISECONDS );
        this.latestFinishTimeAsUnit = unit.convert( latestFinishTimeAsMilli, TimeUnit.MILLISECONDS );
        this.totalRunDurationAsUnit = unit.convert( latestFinishTimeAsMilli - startTimeAsMilli, TimeUnit.MILLISECONDS );
        this.throughput = 1000 * (operationCount / (double) unit.toMillis( totalRunDurationAsUnit ));
        this.operationCount = operationCount;
        this.unit = unit;
    }

    @JsonProperty( value = "all_metrics" )
    public List<OperationMetricsSnapshot> allMetrics()
    {
        return metrics;
    }

    @JsonProperty( value = "all_metrics" )
    private void setAllMetrics( List<OperationMetricsSnapshot> metrics )
    {
        this.metrics = metrics;
        Collections.sort( metrics, new OperationTypeMetricsManager.OperationMetricsNameComparator() );
    }

    public long startTimeAsMilli()
    {
        return unit.toMillis( startTimeAsUnit );
    }

    public long latestFinishTimeAsMilli()
    {
        return unit.toMillis( latestFinishTimeAsUnit );
    }

    public long totalRunDurationAsNano()
    {
        return unit.toNanos( totalRunDurationAsUnit );
    }

    public long totalOperationCount()
    {
        return operationCount;
    }

    public double throughput()
    {
        return throughput;
    }

    public String toJson()
    {
        try
        {
            return new ObjectMapper().writer( new DefaultPrettyPrinter() ).writeValueAsString( this );
        }
        catch ( Exception e )
        {
            System.out.println( ConcurrentErrorReporter.stackTraceToString( e ) );
            throw new RuntimeException( "Unable to generate parameter values string", e );
        }
    }

    @Override
    public String toString()
    {
        return "WorkloadResultsSnapshot{" +
               "metrics=" + metrics +
               ", unit=" + unit +
               ", startTimeAsUnit=" + startTimeAsUnit +
               ", latestFinishTimeAsUnit=" + latestFinishTimeAsUnit +
               ", totalRunDurationAsUnit=" + totalRunDurationAsUnit +
               ", operationCount=" + operationCount +
               ", throughput=" + throughput +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        WorkloadResultsSnapshot that = (WorkloadResultsSnapshot) o;

        if ( latestFinishTimeAsUnit != that.latestFinishTimeAsUnit )
        { return false; }
        if ( operationCount != that.operationCount )
        { return false; }
        if ( startTimeAsUnit != that.startTimeAsUnit )
        { return false; }
        if ( totalRunDurationAsUnit != that.totalRunDurationAsUnit )
        { return false; }
        if ( metrics != null ? !metrics.equals( that.metrics ) : that.metrics != null )
        { return false; }
        if ( unit != that.unit )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = metrics != null ? metrics.hashCode() : 0;
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        result = 31 * result + (int) (startTimeAsUnit ^ (startTimeAsUnit >>> 32));
        result = 31 * result + (int) (latestFinishTimeAsUnit ^ (latestFinishTimeAsUnit >>> 32));
        result = 31 * result + (int) (totalRunDurationAsUnit ^ (totalRunDurationAsUnit >>> 32));
        result = 31 * result + (int) (operationCount ^ (operationCount >>> 32));
        return result;
    }
}
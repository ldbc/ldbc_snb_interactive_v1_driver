package com.ldbc.driver.measurements;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ldbc.driver.measurements.exporter.MeasurementsExporter;

/**
 * Collects latency measurements, and reports them when requested
 * 
 * @author cooperb
 */

public class Measurements
{
    private static Logger logger = Logger.getLogger( Measurements.class );

    private final Map<String, String> properties;
    private final HashMap<String, OneMeasurement> data;
    private final Class<? extends OneMeasurement> oneMeasurementClass;

    public Measurements( Class<? extends OneMeasurement> oneMeasurementClass, Map<String, String> properties )
    {
        // TODO remove need for passing in properties
        this.properties = properties;

        this.data = new HashMap<String, OneMeasurement>();
        this.oneMeasurementClass = oneMeasurementClass;
    }

    private OneMeasurement constructOneMeasurement( String name ) throws MeasurementsException
    {
        try
        {
            return oneMeasurementClass.getConstructor( String.class, Map.class ).newInstance( name, properties );
        }
        catch ( Exception e )
        {
            String errMsg = String.format( "Error instantiating OneMeasurement [%s]", oneMeasurementClass.getName() );
            logger.error( errMsg, e );
            throw new MeasurementsException( errMsg, e.getCause() );
        }
    }

    /**
     * Report a single value of a single metric. E.g. for read latency,
     * operation="READ" and latency is the measured value.
     */
    public void measure( String operation, int latency ) throws MeasurementsException
    {
        if ( false == data.containsKey( operation ) )
        {
            data.put( operation, constructOneMeasurement( operation ) );
        }
        data.get( operation ).measure( latency );
    }

    public void reportReturnCode( String operation, int code ) throws MeasurementsException
    {
        if ( false == data.containsKey( operation ) )
        {
            data.put( operation, constructOneMeasurement( operation ) );
        }
        data.get( operation ).reportReturnCode( code );
    }

    /**
     * Export current measurements to specified format
     */
    public void exportMeasurements( MeasurementsExporter exporter ) throws MeasurementsException
    {
        for ( OneMeasurement measurement : data.values() )
        {
            measurement.exportMeasurements( exporter );
        }
    }

    /**
     * Return a one line summary of the measurements
     */
    public String getSummary()
    {
        StringBuilder summary = new StringBuilder();
        for ( OneMeasurement m : data.values() )
        {
            summary.append( m.getSummary() + " " );
        }
        return summary.toString();
    }
}

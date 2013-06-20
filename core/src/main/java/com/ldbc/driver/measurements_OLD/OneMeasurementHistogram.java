/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.ldbc.driver.measurements_OLD;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import com.ldbc.driver.measurements_OLD.exporter_OLD.MeasurementsExporter;
import com.ldbc.driver.util.MapUtils;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ
 * LATENCY.
 * 
 * @author cooperb
 * 
 */
public class OneMeasurementHistogram extends OneMeasurement
{
    private final String BUCKETS = "histogram.buckets";
    private final String BUCKETS_DEFAULT = "1000";

    private int buckets;
    private int[] histogram;
    private int histogramOverflow;
    private int operations;
    private long totalLatency;

    // keep a windowed version of these stats for printing status
    private int windowOperations;
    private long windowTotalLatency;

    private int min;
    private int max;
    private HashMap<Integer, int[]> returnCodes;

    public OneMeasurementHistogram( String name, Map<String, String> properties )
    {
        super( name );
        buckets = Integer.parseInt( MapUtils.getDefault( properties, BUCKETS, BUCKETS_DEFAULT ) );
        histogram = new int[buckets];
        histogramOverflow = 0;
        operations = 0;
        totalLatency = 0;
        windowOperations = 0;
        windowTotalLatency = 0;
        min = -1;
        max = -1;
        returnCodes = new HashMap<Integer, int[]>();
    }

    @Override
    public void reportReturnCode( int code )
    {
        Integer Icode = code;
        if ( !returnCodes.containsKey( Icode ) )
        {
            int[] val = new int[1];
            val[0] = 0;
            returnCodes.put( Icode, val );
        }
        returnCodes.get( Icode )[0]++;
    }

    @Override
    public void measure( int latency )
    {
        if ( latency / 1000 >= buckets )
        {
            histogramOverflow++;
        }
        else
        {
            histogram[latency / 1000]++;
        }
        operations++;
        totalLatency += latency;
        windowOperations++;
        windowTotalLatency += latency;

        if ( ( min < 0 ) || ( latency < min ) )
        {
            min = latency;
        }

        if ( ( max < 0 ) || ( latency > max ) )
        {
            max = latency;
        }
    }

    @Override
    public void exportMeasurements( MeasurementsExporter exporter ) throws MeasurementsException
    {
        exporter.write( getName(), "Operations", operations );
        exporter.write( getName(), "AverageLatency(us)", ( ( (double) totalLatency ) / ( (double) operations ) ) );
        exporter.write( getName(), "MinLatency(us)", min );
        exporter.write( getName(), "MaxLatency(us)", max );

        int opcounter = 0;
        boolean done95th = false;
        for ( int i = 0; i < buckets; i++ )
        {
            opcounter += histogram[i];
            if ( ( !done95th ) && ( ( (double) opcounter ) / ( (double) operations ) >= 0.95 ) )
            {
                exporter.write( getName(), "95thPercentileLatency(ms)", i );
                done95th = true;
            }
            if ( ( (double) opcounter ) / ( (double) operations ) >= 0.99 )
            {
                exporter.write( getName(), "99thPercentileLatency(ms)", i );
                break;
            }
        }

        for ( Integer I : returnCodes.keySet() )
        {
            int[] val = returnCodes.get( I );
            exporter.write( getName(), "Return=" + I, val[0] );
        }

        for ( int i = 0; i < buckets; i++ )
        {
            exporter.write( getName(), Integer.toString( i ), histogram[i] );
        }
        exporter.write( getName(), ">" + buckets, histogramOverflow );
    }

    @Override
    public int getOperationCount()
    {
        return operations;
    }

    @Override
    public String getSummary()
    {
        if ( windowOperations == 0 )
        {
            return "";
        }
        DecimalFormat d = new DecimalFormat( "#.##" );
        double report = ( (double) windowTotalLatency ) / ( (double) windowOperations );
        windowTotalLatency = 0;
        windowOperations = 0;
        return "[" + getName() + " AverageLatency(us)=" + d.format( report ) + "]";
    }

}

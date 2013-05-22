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
package com.ldbc.driver.measurements.exporter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import com.ldbc.driver.measurements.MeasurementsException;

/**
 * Export measurements into a machine readable JSON file.
 */
public class JSONMeasurementsExporter implements MeasurementsExporter
{

    private JsonFactory factory = new JsonFactory();
    private JsonGenerator g;

    public JSONMeasurementsExporter( OutputStream os ) throws IOException
    {

        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( os ) );
        g = factory.createJsonGenerator( bw );
        g.setPrettyPrinter( new DefaultPrettyPrinter() );
    }

    public void write( String metric, String measurement, int i ) throws MeasurementsException
    {
        try
        {
            g.writeStartObject();
            g.writeStringField( "metric", metric );
            g.writeStringField( "measurement", measurement );
            g.writeNumberField( "value", i );
            g.writeEndObject();
        }
        catch ( Exception e )
        {
            String errMsg = String.format( "Error writing measurement - metric[%s], measurement[%s], i[%s]", metric,
                    measurement, i );
            throw new MeasurementsException( errMsg, e.getCause() );
        }
    }

    public void write( String metric, String measurement, double d ) throws MeasurementsException
    {
        try
        {
            g.writeStartObject();
            g.writeStringField( "metric", metric );
            g.writeStringField( "measurement", measurement );
            g.writeNumberField( "value", d );
            g.writeEndObject();
        }
        catch ( Exception e )
        {
            String errMsg = String.format( "Error writing measurement - metric[%s], measurement[%s], d[%s]", metric,
                    measurement, d );
            throw new MeasurementsException( errMsg, e.getCause() );
        }
    }

    public void close() throws IOException
    {
        if ( g != null )
        {
            g.close();
        }
    }

}

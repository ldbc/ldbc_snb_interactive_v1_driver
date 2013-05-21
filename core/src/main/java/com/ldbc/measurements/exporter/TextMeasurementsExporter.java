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
package com.ldbc.measurements.exporter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import com.ldbc.measurements.MeasurementsException;

/**
 * Write human readable text. Tries to emulate the previous print report method.
 */
public class TextMeasurementsExporter implements MeasurementsExporter
{

    private BufferedWriter bw;

    public TextMeasurementsExporter( OutputStream os )
    {
        this.bw = new BufferedWriter( new OutputStreamWriter( os ) );
    }

    public void write( String metric, String measurement, int i ) throws MeasurementsException
    {
        try
        {
            bw.write( "[" + metric + "], " + measurement + ", " + i );
            bw.newLine();
        }
        catch ( IOException e )
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
            bw.write( "[" + metric + "], " + measurement + ", " + d );
            bw.newLine();
        }
        catch ( IOException e )
        {
            String errMsg = String.format( "Error writing measurement - metric[%s], measurement[%s], d[%s]", metric,
                    measurement, d );
            throw new MeasurementsException( errMsg, e.getCause() );
        }
    }

    public void close() throws IOException
    {
        this.bw.close();
    }

}

/**                                                                                                                                                                                
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

package com.yahoo.ycsb.graph.generator;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;

import com.yahoo.ycsb.WorkloadException;

/**
 * A generator, whose sequence is the lines of a file.
 */
public class FileGenerator extends Generator<String>
{
    String filename;
    BufferedReader reader;

    /**
     * Create a FileGenerator with the given file.
     * 
     * @param _filename The file to read lines from.
     * @throws WorkloadException
     */
    public FileGenerator( String _filename ) throws WorkloadException
    {
        try
        {
            filename = _filename;
            File file = new File( filename );
            FileInputStream in = new FileInputStream( file );
            reader = new BufferedReader( new InputStreamReader( in ) );
        }
        catch ( IOException e )
        {
            throw new WorkloadException( String.format( "Error creating FileGenerator : %s", _filename, last() ),
                    e.getCause() );
        }
    }

    /**
     * Return the next string of the sequence, ie the next line of the file.
     * 
     * @throws WorkloadException
     */
    @Override
    protected String doNext() throws WorkloadException
    {
        return readNextLine();
    }

    private synchronized String readNextLine() throws WorkloadException
    {
        try
        {
            return reader.readLine();
        }
        catch ( NullPointerException e )
        {
            throw new WorkloadException( String.format( "Error encountered reading next line\nFile : %s\nLine : %s",
                    filename, last() ), e.getCause() );
        }
        catch ( IOException e )
        {
            throw new WorkloadException( String.format( "Error encountered reading next line\nFile : %s\nLine : %s",
                    filename, last() ), e.getCause() );
        }
    }

    /**
     * Reopen the file to reuse values.
     * 
     * @throws WorkloadException
     */
    public synchronized void reloadFile() throws WorkloadException
    {
        try
        {
            System.err.println( "Reload " + filename );
            reader.close();
            File file = new File( filename );
            FileInputStream in = new FileInputStream( file );
            reader = new BufferedReader( new InputStreamReader( in ) );
        }
        catch ( IOException e )
        {
            throw new WorkloadException( String.format( "Error encountered reloading file : %s", filename ),
                    e.getCause() );
        }
    }
}

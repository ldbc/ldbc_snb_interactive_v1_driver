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

package com.ldbc.generator.ycsb;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorException;

/**
 * A generator, whose sequence is the lines of a file.
 */
public class YcsbFileGenerator extends Generator<String>
{
    String filename;
    BufferedReader reader;

    /**
     * Create a FileGenerator with the given file.
     * 
     * @param filename The file to read lines from.
     */
    public YcsbFileGenerator( RandomDataGenerator random, String filename ) throws GeneratorException
    {
        super( random );
        try
        {
            this.filename = filename;
            File file = new File( filename );
            FileInputStream in = new FileInputStream( file );
            this.reader = new BufferedReader( new InputStreamReader( in ) );
        }
        catch ( IOException e )
        {
            throw new GeneratorException( String.format( "Error creating FileGenerator : %s", filename, last() ),
                    e.getCause() );
        }
    }

    /**
     * Return the next string of the sequence, ie the next line of the file.
     */
    @Override
    protected String doNext() throws GeneratorException
    {
        return readNextLine();
    }

    private synchronized String readNextLine() throws GeneratorException
    {
        try
        {
            return reader.readLine();
        }
        catch ( NullPointerException e )
        {
            throw new GeneratorException( String.format( "Error encountered reading next line\nFile : %s\nLine : %s",
                    filename, last() ), e.getCause() );
        }
        catch ( IOException e )
        {
            throw new GeneratorException( String.format( "Error encountered reading next line\nFile : %s\nLine : %s",
                    filename, last() ), e.getCause() );
        }
    }

    /**
     * Reopen the file to reuse values.
     */
    public synchronized void reloadFile() throws GeneratorException
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
            throw new GeneratorException( String.format( "Error encountered reloading file : %s", filename ),
                    e.getCause() );
        }
    }
}

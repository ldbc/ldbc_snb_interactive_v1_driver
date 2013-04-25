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
package com.yahoo.ycsb.generator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.Pair;

/**
 * Histogram buckets are of width one, but the values are multiplied by a block
 * size. Therefore, instead of drawing sizes uniformly at random within each
 * bucket, we always draw the largest value in the current bucket, so the value
 * drawn is always a multiple of block_size.
 * 
 * The minimum value this distribution returns is block_size (not zero).
 * 
 * Modified Nov 19 2010 by sears
 * 
 * @author snjones
 * 
 */
public class HistogramGenerator extends Generator<Long>
{

    long blockSize;
    Long[] buckets;
    long area;
    long weightedArea = 0;
    double meanSize = 0;

    HistogramGenerator( RandomDataGenerator random, String histogramFilePath )
    {
        this( random, loadHistogramFromFile( histogramFilePath ) );
    }

    HistogramGenerator( RandomDataGenerator random, Long[] buckets, Long blockSize )
    {
        this( random, new Pair<Long[], Long>( buckets, blockSize ) );
    }

    private HistogramGenerator( RandomDataGenerator random, Pair<Long[], Long> histogram )
    {
        super( random );
        this.buckets = histogram._1();
        this.blockSize = histogram._2();
        init();
    }

    private void init()
    {
        for ( int i = 0; i < buckets.length; i++ )
        {
            area += buckets[i];
            weightedArea = i * buckets[i];
        }
        // calculate average file size
        meanSize = ( (double) blockSize ) * ( (double) weightedArea ) / (double) ( area );
    }

    // TODO return a Histogram class instead of pair
    private static Pair<Long[], Long> loadHistogramFromFile( String histogramFilePath )
    {
        try
        {
            Long[] tempBuckets = null;
            Long tempBlockSize = null;

            BufferedReader histogramFileReader = new BufferedReader( new FileReader( histogramFilePath ) );
            String histogramFileLine;
            String[] histogramFileLineTokens;

            ArrayList<Integer> bucketValueList = new ArrayList<Integer>();

            histogramFileLine = histogramFileReader.readLine();
            if ( histogramFileLine == null )
            {
                throw new GeneratorException( "Empty input file!\n" );
            }
            histogramFileLineTokens = histogramFileLine.split( "\t" );
            if ( histogramFileLineTokens[0].equals( "BlockSize" ) == false )
            {
                throw new GeneratorException( "First line of histogram is not the BlockSize!\n" );
            }
            tempBlockSize = Long.parseLong( histogramFileLineTokens[1] );

            while ( ( histogramFileLine = histogramFileReader.readLine() ) != null )
            {
                // [0] is the bucket, [1] is the value
                histogramFileLineTokens = histogramFileLine.split( "\t" );

                int bucket = Integer.parseInt( histogramFileLineTokens[0] );
                int value = Integer.parseInt( histogramFileLineTokens[1] );
                bucketValueList.add( bucket, value );
            }

            tempBuckets = bucketValueList.toArray( new Long[bucketValueList.size()] );

            histogramFileReader.close();

            return new Pair<Long[], Long>( tempBuckets, tempBlockSize );
        }
        catch ( FileNotFoundException fnfe )
        {
            throw new GeneratorException( "Histogram file not found: " + histogramFilePath, fnfe.getCause() );
        }
        catch ( IOException ioe )
        {
            throw new GeneratorException( "Could not load histogram file", ioe.getCause() );
        }
    }

    @Override
    protected Long doNext()
    {
        int number = getRandom().nextInt( 0, (int) area );
        int i;

        for ( i = 0; i < ( buckets.length - 1 ); i++ )
        {
            number -= buckets[i];
            if ( number <= 0 )
            {
                return (long) ( ( i + 1 ) * blockSize );
            }
        }

        return (long) ( i * blockSize );
    }

    // public double mean()
    // {
    // return mean_size;
    // }
}

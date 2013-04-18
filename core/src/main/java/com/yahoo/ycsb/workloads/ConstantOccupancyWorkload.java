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
package com.yahoo.ycsb.workloads;

import java.util.Map;

import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.GeneratorFactory;

/**
 * A disk-fragmenting workload.
 * <p>
 * Properties to control the client:
 * </p>
 * <UL>
 * <LI><b>disksize</b>: how many bytes of storage can the disk store? (default
 * 100,000,000)
 * <LI><b>occupancy</b>: what fraction of the available storage should be used?
 * (default 0.9)
 * <LI><b>requestdistribution</b>: what distribution should be used to select
 * the records to operate on - uniform, zipfian or latest (default: histogram)
 * </ul>
 * 
 * 
 * <p>
 * See also: Russell Sears, Catharine van Ingen. <a href=
 * 'https://database.cs.wisc.edu/cidr/cidr2007/papers/cidr07p34.pdf'>Fragmentati
 * o n in Large Object Repositories</a>, CIDR 2006. [<a href=
 * 'https://database.cs.wisc.edu/cidr/cidr2007/slides/p34-sears.ppt'>Presentatio
 * n < / a > ]
 * </p>
 * 
 * 
 * @author sears
 * 
 */
public class ConstantOccupancyWorkload extends CoreWorkload
{
    long diskSize;
    long storageAges;
    Generator<Integer> objectSizes;
    double occupancy;

    long objectCount;

    public static final String STORAGE_AGE_PROPERTY = "storageages";
    public static final long STORAGE_AGE_PROPERTY_DEFAULT = 10;

    public static final String DISK_SIZE_PROPERTY = "disksize";
    public static final long DISK_SIZE_PROPERTY_DEFAULT = 100 * 1000 * 1000;

    public static final String OCCUPANCY_PROPERTY = "occupancy";
    public static final double OCCUPANCY_PROPERTY_DEFAULT = 0.9;

    @Override
    public void init( Map<String, String> properties, GeneratorFactory generatorFactory ) throws WorkloadException
    {
        super.init( properties, generatorFactory );

        diskSize = Long.parseLong( Utils.mapGetDefault( properties, DISK_SIZE_PROPERTY,
                Long.toString( DISK_SIZE_PROPERTY_DEFAULT ) ) );
        storageAges = Long.parseLong( Utils.mapGetDefault( properties, STORAGE_AGE_PROPERTY,
                Long.toString( STORAGE_AGE_PROPERTY_DEFAULT ) ) );
        occupancy = Double.parseDouble( Utils.mapGetDefault( properties, OCCUPANCY_PROPERTY,
                Double.toString( OCCUPANCY_PROPERTY_DEFAULT ) ) );

        if ( properties.get( Client.RECORD_COUNT ) != null || properties.get( Client.INSERT_COUNT ) != null
             || properties.get( Client.OPERATION_COUNT ) != null )
        {
            System.err.println( "Warning: record, insert or operation count was set prior to initting ConstantOccupancyWorkload.  Overriding old values." );
        }

        Distribution fieldLengthDistribution = Distribution.valueOf( Utils.mapGetDefault( properties,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION,
                CoreWorkloadProperties.FIELD_LENGTH_DISTRIBUTION_DEFAULT ).toUpperCase() );
        long fieldLength = Integer.parseInt( Utils.mapGetDefault( properties, CoreWorkloadProperties.FIELD_LENGTH,
                CoreWorkloadProperties.FIELD_LENGTH_DEFAULT ) );
        String fieldLengthHistogramFilePath = Utils.mapGetDefault( properties,
                CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE,
                CoreWorkloadProperties.FIELD_LENGTH_HISTOGRAM_FILE_DEFAULT );
        Generator<Long> g = WorkloadUtils.buildFieldLengthGenerator( fieldLengthDistribution, 1l, fieldLength,
                fieldLengthHistogramFilePath );
        // TODO is mean() necessary? set to constant for now, fix later
        double fieldsize = fieldLength / 2;
        // double fieldsize = g.mean();

        int fieldcount = Integer.parseInt( Utils.mapGetDefault( properties, CoreWorkloadProperties.FIELD_COUNT,
                CoreWorkloadProperties.FIELD_COUNT_DEFAULT ) );
        objectCount = (long) ( occupancy * ( (double) diskSize / ( fieldsize * (double) fieldcount ) ) );
        if ( objectCount == 0 )
        {
            throw new IllegalStateException( "Object count was zero.  Perhaps disksize is too low?" );
        }
        properties.put( Client.RECORD_COUNT, Long.toString( objectCount ) );
        properties.put( Client.OPERATION_COUNT, Long.toString( storageAges * objectCount ) );
        properties.put( Client.INSERT_COUNT, Long.toString( objectCount ) );
    }
}

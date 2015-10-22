package com.ldbc.driver.workloads.ldbc.snb.bi;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.CharSeekerParams;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.GeneratorFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class Query2EventStreamReader extends BaseEventStreamReader
{
    public Query2EventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersInputStream, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery2TopTags(
                (long) parameters[0],
                (long) parameters[1],
                (List<String>) parameters[2],
                (int) parameters[3],
                (long) parameters[4],
                (int) parameters[5]
        );
    }

    @Override
    CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder()
    {
        return new CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>()
        {
            /*
            Date0|Date1|CountryA|CountryB
            1236219|1335225600|countryA|countryB
             */
            @Override
            public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters,
                    Mark mark )
                    throws IOException
            {
                long date0;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    date0 = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                long date1;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    date1 = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving date" );
                }

                List<String> countries;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    countries = Lists.newArrayList( charSeeker.extract( mark, extractors.stringArray() ).value() );
                }
                else
                {
                    throw new GeneratorException( "Error retrieving languages" );
                }

                long endOfSimulationTime;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    endOfSimulationTime = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving end of simulation time" );
                }

                int messageThreshold;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    messageThreshold = charSeeker.extract( mark, extractors.int_() ).intValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving min message count" );
                }

                return new Object[]{
                        date0,
                        date1,
                        countries,
                        messageThreshold,
                        endOfSimulationTime,
                        LdbcSnbBiQuery2TopTags.DEFAULT_LIMIT,
                };
            }
        };
    }

    @Override
    int columnCount()
    {
        return 5;
    }
}

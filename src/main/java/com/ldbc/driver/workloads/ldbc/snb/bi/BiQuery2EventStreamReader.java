package com.ldbc.driver.workloads.ldbc.snb.bi;


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

public class BiQuery2EventStreamReader extends BaseEventStreamReader
{
    public BiQuery2EventStreamReader(
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
                (String) parameters[2],
                (String) parameters[3],
                (int) parameters[4]
        );
    }

    @Override
    CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder()
    {
        return new CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>()
        {
            @Override
            public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters,
                    Mark mark )
                    throws IOException
            {
                long startDate;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    startDate = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                long endDate;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    endDate = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving endDate" );
                }

                String country1;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    country1 = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving country1" );
                }

                String country2;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    country2 = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving country2" );
                }

                return new Object[]{
                        startDate,
                        endDate,
                        country1,
                        country2,
                        LdbcSnbBiQuery2TopTags.DEFAULT_LIMIT,
                };
            }
        };
    }

    @Override
    int columnCount()
    {
        return 4;
    }
}

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

public class BiQuery16EventStreamReader extends BaseEventStreamReader
{
    public BiQuery16EventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersInputStream, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery16FakeNewsDetection(
                (String) parameters[0],
                (long) parameters[1],
                (String) parameters[2],
                (long) parameters[3],
                (int) parameters[4],
                (int) parameters[5]
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
                String tagA;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tagA = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                long dateA;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    dateA = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving dateA" );
                }

                String tagB;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tagB = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving tagB" );
                }

                long dateB;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    dateB = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving dateB" );
                }

                int maxKnowsLimit;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    maxKnowsLimit = charSeeker.extract( mark, extractors.int_() ).intValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving limit" );
                }

                int limit;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    limit = charSeeker.extract( mark, extractors.int_() ).intValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving limit" );
                }

                return new Object[]{tagA, dateA, tagB, dateB, maxKnowsLimit, limit};
            }
        };
    }

    @Override
    int columnCount()
    {
        return 6;
    }
}

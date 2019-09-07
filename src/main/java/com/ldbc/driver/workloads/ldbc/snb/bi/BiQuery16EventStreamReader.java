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
        return new LdbcSnbBiQuery16ExpertsInSocialCircle(
                (long) parameters[0],
                (String) parameters[1],
                (String) parameters[2],
                (int) parameters[3],
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
                long personId;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    personId = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                String country;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    country = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving country name" );
                }

                String tagClass;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tagClass = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving tag class" );
                }

                int minPathDistance;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    minPathDistance = charSeeker.extract( mark, extractors.int_() ).intValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving min path distance" );
                }

                int maxPathDistance;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    maxPathDistance = charSeeker.extract( mark, extractors.int_() ).intValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving max path distance" );
                }

                return new Object[]{personId, country, tagClass, minPathDistance, maxPathDistance,
                        LdbcSnbBiQuery16ExpertsInSocialCircle.DEFAULT_LIMIT};
            }
        };
    }

    @Override
    int columnCount()
    {
        return 5;
    }
}

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

public class Query9EventStreamReader extends BaseEventStreamReader
{
    public Query9EventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersInputStream, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery9RelatedForums(
                (String) parameters[0],
                (String) parameters[1],
                (int) parameters[2],
                (int) parameters[3]
        );
    }

    @Override
    CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder()
    {
        return new CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>()
        {
            /*
            TagClassA|CountryB
            tag_a|tag_b
            */
            @Override
            public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters,
                    Mark mark )
                    throws IOException
            {
                String tagClassA;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tagClassA = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                String tagClassB;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tagClassB = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving country name" );
                }

                int threshold;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    threshold = charSeeker.extract( mark, extractors.int_() ).intValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving threshold" );
                }

                return new Object[]{tagClassA, tagClassB, threshold, LdbcSnbBiQuery9RelatedForums.DEFAULT_LIMIT};
            }
        };
    }

    @Override
    int columnCount()
    {
        return 3;
    }
}

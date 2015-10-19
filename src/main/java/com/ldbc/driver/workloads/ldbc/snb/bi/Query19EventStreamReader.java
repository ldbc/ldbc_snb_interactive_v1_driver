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

public class Query19EventStreamReader extends BaseEventStreamReader
{
    public Query19EventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersInputStream, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery19StrangerInteraction(
                (long) parameters[0],
                (String) parameters[1],
                (String) parameters[2],
                (int) parameters[3]
        );
    }

    @Override
    CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder()
    {
        return new CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>()
        {
            /*
            TagClass0|TagClass1
            names|places
            */
            @Override
            public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters,
                    Mark mark )
                    throws IOException
            {
                long date;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    date = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                String tagClass0;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tagClass0 = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving tag class 0" );
                }

                String tagClass1;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tagClass1 = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving tag class 1" );
                }

                return new Object[]{date, tagClass0, tagClass1, LdbcSnbBiQuery19StrangerInteraction.DEFAULT_LIMIT};
            }
        };
    }

    @Override
    int columnCount()
    {
        return 3;
    }
}

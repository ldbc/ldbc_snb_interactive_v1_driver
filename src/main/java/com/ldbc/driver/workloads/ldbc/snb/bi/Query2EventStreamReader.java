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

import java.io.File;
import java.io.IOException;

public class Query2EventStreamReader extends BaseEventStreamReader
{
    public Query2EventStreamReader(
            File parametersFile,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersFile, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery2(
                (long) parameters[0],
                (long) parameters[1],
                (int) parameters[2]
        );
    }

    @Override
    CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder()
    {
        return new CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>()
        {
            /*
            Date0|Date1
            1236219|1335225600
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

                return new Object[]{date0, date1, LdbcSnbBiQuery2.DEFAULT_LIMIT};
            }
        };
    }

    @Override
    int columnCount()
    {
        return 2;
    }
}

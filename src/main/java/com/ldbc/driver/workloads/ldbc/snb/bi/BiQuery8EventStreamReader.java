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

public class BiQuery8EventStreamReader extends BaseEventStreamReader
{
    public BiQuery8EventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersInputStream, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery8TagPerson(
                (String) parameters[0],
                (long) parameters[1],
                (int) parameters[2]
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
                String tag;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tag = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                long date;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    date = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving date" );
                }
                return new Object[]{tag, date, LdbcSnbBiQuery8TagPerson.DEFAULT_LIMIT};
            }
        };
    }

    @Override
    int columnCount()
    {
        return 2;
    }
}

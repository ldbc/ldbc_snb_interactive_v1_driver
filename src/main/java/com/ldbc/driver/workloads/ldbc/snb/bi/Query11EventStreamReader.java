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

public class Query11EventStreamReader extends BaseEventStreamReader
{
    public Query11EventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersInputStream, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery11UnrelatedReplies(
                (String) parameters[0],
                (List<String>) parameters[1],
                (int) parameters[2]
        );
    }

    @Override
    CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder()
    {
        return new CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>()
        {
            /*
            KeyWord|Country
            Chicken|Egypt
            */
            @Override
            public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters,
                    Mark mark )
                    throws IOException
            {
                String country;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    country = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                List<String> blackList;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    blackList = Lists.newArrayList( charSeeker.extract( mark, extractors.stringArray() ).value() );
                }
                else
                {
                    throw new GeneratorException( "Error retrieving black list" );
                }

                return new Object[]{country, blackList, LdbcSnbBiQuery11UnrelatedReplies.DEFAULT_LIMIT};
            }
        };
    }

    @Override
    int columnCount()
    {
        return 2;
    }
}

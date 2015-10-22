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

public class Query3EventStreamReader extends BaseEventStreamReader
{
    public Query3EventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersInputStream, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery3TagEvolution(
                (long) parameters[0],
                (long) parameters[1],
                (long) parameters[2],
                (long) parameters[3],
                (int) parameters[4]
        );
    }

    @Override
    CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder()
    {
        return new CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>()
        {
            /*
            range1Start|range1End|range2Start|range2End
            7696581543848|1293840000|1293840000|1293840000
             */
            @Override
            public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters,
                    Mark mark )
                    throws IOException
            {
                long range1Start;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    range1Start = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                long range1End;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    range1End = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving date" );
                }

                long range2Start;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    range2Start = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving date" );
                }

                long range2End;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    range2End = charSeeker.extract( mark, extractors.long_() ).longValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving date" );
                }

                return new Object[]{
                        range1Start,
                        range1End,
                        range2Start,
                        range2End,
                        LdbcSnbBiQuery3TagEvolution.DEFAULT_LIMIT
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

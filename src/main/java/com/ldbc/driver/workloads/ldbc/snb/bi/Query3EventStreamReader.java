package com.ldbc.driver.workloads.ldbc.snb.bi;


import com.ldbc.driver.Operation;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.String.format;

public class Query3EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> csvRows;

    public Query3EventStreamReader( Iterator<Object[]> csvRows )
    {
        this.csvRows = csvRows;
    }

    @Override
    public boolean hasNext()
    {
        return csvRows.hasNext();
    }

    @Override
    public Operation next()
    {
        Object[] rowAsObjects = csvRows.next();
        Operation operation = new LdbcSnbBiQuery3(
                (long) rowAsObjects[0],
                (long) rowAsObjects[1],
                (int) rowAsObjects[2]
        );
        operation.setDependencyTimeStamp( 0 );
        return operation;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    public static class Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>
    {
        /*
        Date0|Date1
        7696581543848|1293840000
         */
        @Override
        public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark )
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

            return new Object[]{date0, date1, LdbcSnbBiQuery3.DEFAULT_LIMIT};
        }
    }
}

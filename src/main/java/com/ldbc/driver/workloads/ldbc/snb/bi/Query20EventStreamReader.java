package com.ldbc.driver.workloads.ldbc.snb.bi;


import com.ldbc.driver.Operation;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.String.format;

public class Query20EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> csvRows;

    public Query20EventStreamReader( Iterator<Object[]> csvRows )
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
        Operation operation = new LdbcSnbBiQuery20(
                (int) rowAsObjects[0]
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
        @Override
        public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark )
                throws IOException
        {
            return new Object[]{LdbcSnbBiQuery20.DEFAULT_LIMIT};
        }
    }
}

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

public class Query22EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> csvRows;

    public Query22EventStreamReader( Iterator<Object[]> csvRows )
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
        Operation operation = new LdbcSnbBiQuery22(
                (String) rowAsObjects[0],
                (String) rowAsObjects[1],
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
        Country0|Country1
        New Zealand| England
        */
        @Override
        public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark )
                throws IOException
        {
            String country0;
            if ( charSeeker.seek( mark, columnDelimiters ) )
            {
                country0 = charSeeker.extract( mark, extractors.string() ).value();
            }
            else
            {
                // if first column of next row contains nothing it means the file is finished
                return null;
            }

            String country1;
            if ( charSeeker.seek( mark, columnDelimiters ) )
            {
                country1 = charSeeker.extract( mark, extractors.string() ).value();
            }
            else
            {
                throw new GeneratorException( "Error retrieving country name" );
            }

            return new Object[]{country0, country1, LdbcSnbBiQuery22.DEFAULT_LIMIT};
        }
    }
}

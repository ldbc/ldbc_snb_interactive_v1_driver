package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.ldbc.driver.Operation;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.String.format;

public class Query1EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> csvRows;

    public Query1EventStreamReader( Iterator<Object[]> csvRows )
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
        Operation operation = new LdbcQuery1(
                (long) rowAsObjects[0],
                (String) rowAsObjects[1],
                LdbcQuery1.DEFAULT_LIMIT
        );
        operation.setDependencyTimeStamp( 0 );
        return operation;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    public static class Query1Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>
    {
        /*
        Person|Name
        2199032251700|Andrea
         */
        @Override
        public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark )
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

            String personName;
            if ( charSeeker.seek( mark, columnDelimiters ) )
            {
                personName = charSeeker.extract( mark, extractors.string() ).value();
            }
            else
            {
                throw new GeneratorException( "Error retrieving person name" );
            }

            return new Object[]{personId, personName};
        }
    }
}

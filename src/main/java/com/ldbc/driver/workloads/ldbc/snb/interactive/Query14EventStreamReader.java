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

public class Query14EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> csvRows;

    public Query14EventStreamReader( Iterator<Object[]> csvRows )
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
        Operation operation = new LdbcQuery14(
                (long) rowAsObjects[0],
                (long) rowAsObjects[1]
        );
        operation.setDependencyTimeStamp( 0 );
        return operation;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    public static class Query14Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>
    {
        /*
        person1Id|person2Id
        15393166495097|2199027958081
        */
        @Override
        public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark )
                throws IOException
        {
            long person1Id;
            if ( charSeeker.seek( mark, columnDelimiters ) )
            {
                person1Id = charSeeker.extract( mark, extractors.long_() ).longValue();
            }
            else
            {
                // if first column of next row contains nothing it means the file is finished
                return null;
            }

            long person2Id;
            if ( charSeeker.seek( mark, columnDelimiters ) )
            {
                person2Id = charSeeker.extract( mark, extractors.long_() ).longValue();
            }
            else
            {
                throw new GeneratorException( "Error retrieving person 2 id" );
            }

            return new Object[]{person1Id, person2Id};
        }
    }
}

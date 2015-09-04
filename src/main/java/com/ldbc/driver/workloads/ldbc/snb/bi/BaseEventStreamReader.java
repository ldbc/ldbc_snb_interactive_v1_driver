package com.ldbc.driver.workloads.ldbc.snb.bi;


import com.google.common.base.Charsets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.csv.charseeker.BufferedCharSeeker;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.CharSeekerParams;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.csv.charseeker.Readables;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.NoRemoveIterator;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import static java.lang.String.format;

abstract class BaseEventStreamReader extends NoRemoveIterator<Operation> implements Closeable
{
    private final CharSeeker charSeeker;
    private final InputStream parametersInputStream;
    private final Iterator<Object[]> parametersIterator;

    public BaseEventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        this.parametersInputStream = parametersInputStream;
        charSeeker = new BufferedCharSeeker(
                Readables.wrap(
                        new InputStreamReader( parametersInputStream, Charsets.UTF_8 )
                ),
                charSeekerParams.bufferSize()
        );
        Mark mark = new Mark();
        // skip headers
        try
        {
            for ( int i = 0; i < columnCount(); i++ )
            {
                charSeeker.seek( mark, new int[]{charSeekerParams.columnDelimiter()} );
            }
        }
        catch ( IOException e )
        {
            throw new WorkloadException(
                    format( "Unable to advance parameters stream beyond headers: %s", parametersInputStream ), e );
        }

        parametersIterator = gf.repeating(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        new Extractors( charSeekerParams.arrayDelimiter(), charSeekerParams.tupleDelimiter() ),
                        mark,
                        decoder(),
                        charSeekerParams.columnDelimiter()
                )
        );
    }

    @Override
    public boolean hasNext()
    {
        return parametersIterator.hasNext();
    }

    @Override
    public Operation next()
    {
        Object[] parameters = parametersIterator.next();
        Operation operation = operationFromParameters( parameters );
        operation.setDependencyTimeStamp( 0 );
        return operation;
    }

    @Override
    public void close() throws IOException
    {
        charSeeker.close();
        parametersInputStream.close();
    }

    abstract Operation operationFromParameters( Object[] parameters );

    abstract CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder();

    abstract int columnCount();
}

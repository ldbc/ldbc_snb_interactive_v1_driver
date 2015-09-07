package com.ldbc.driver.generator;

import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.String.format;


public class CsvEventStreamReaderBasicCharSeeker<BASE_EVENT_TYPE> implements Iterator<BASE_EVENT_TYPE>
{
    private final EventDecoder<BASE_EVENT_TYPE> decoder;
    private final CharSeeker charSeeker;
    private final Extractors extractors;
    private final Mark mark;
    private final int[] columnDelimiters;
    private BASE_EVENT_TYPE nextEvent = null;

    public CsvEventStreamReaderBasicCharSeeker(
            CharSeeker charSeeker,
            Extractors extractors,
            Mark mark,
            EventDecoder<BASE_EVENT_TYPE> decoder,
            int columnDelimiter )
    {
        this.charSeeker = charSeeker;
        this.extractors = extractors;
        this.mark = mark;
        this.columnDelimiters = new int[]{columnDelimiter};
        this.decoder = decoder;
    }

    @Override
    public boolean hasNext()
    {
        if ( null == nextEvent )
        {
            nextEvent = getNextEvent();
        }
        return null != nextEvent;
    }

    @Override
    public BASE_EVENT_TYPE next()
    {
        if ( null == nextEvent )
        {
            nextEvent = getNextEvent();
        }
        BASE_EVENT_TYPE result = nextEvent;
        nextEvent = null;
        return result;
    }

    BASE_EVENT_TYPE getNextEvent()
    {
        try
        {
            return decoder.decodeEvent( charSeeker, extractors, columnDelimiters, mark );
        }
        catch ( IOException e )
        {
            throw new GeneratorException( "Error while retrieving next event", e );
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException(
                format( "%s does not support remove()", getClass().getSimpleName() ) );
    }


    public interface EventDecoder<BASE_EVENT_TYPE>
    {
        BASE_EVENT_TYPE decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark )
                throws IOException;
    }
}
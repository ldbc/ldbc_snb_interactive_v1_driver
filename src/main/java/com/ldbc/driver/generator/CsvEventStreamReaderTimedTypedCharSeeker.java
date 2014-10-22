package com.ldbc.driver.generator;


import com.ldbc.driver.util.csv.CharSeeker;
import com.ldbc.driver.util.csv.Extractors;
import com.ldbc.driver.util.csv.Mark;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;


public class CsvEventStreamReaderTimedTypedCharSeeker<BASE_EVENT_TYPE> implements Iterator<BASE_EVENT_TYPE> {
    private final Map<Integer, EventDecoder<BASE_EVENT_TYPE>> decoders;
    private final CharSeeker charSeeker;
    private final Extractors extractors;
    private final Mark mark;
    private final int[] columnDelimiters;
    private BASE_EVENT_TYPE nextEvent = null;

    public CsvEventStreamReaderTimedTypedCharSeeker(CharSeeker charSeeker,
                                                    Extractors extractors,
                                                    Map<Integer, EventDecoder<BASE_EVENT_TYPE>> decoders,
                                                    int columnDelimiter) {
        this.charSeeker = charSeeker;
        this.extractors = extractors;
        this.mark = new Mark();
        this.columnDelimiters = new int[]{columnDelimiter};
        this.decoders = decoders;
    }

    @Override
    public boolean hasNext() {
        if (null == nextEvent) {
            nextEvent = getNextEvent();
        }
        return null != nextEvent;
    }

    @Override
    public BASE_EVENT_TYPE next() {
        if (null == nextEvent) {
            nextEvent = getNextEvent();
        }
        BASE_EVENT_TYPE result = nextEvent;
        nextEvent = null;
        return result;
    }

    BASE_EVENT_TYPE getNextEvent() {
        try {
            long scheduledStartTime;
            if (charSeeker.seek(mark, columnDelimiters)) {
                scheduledStartTime = charSeeker.extract(mark, extractors.long_()).longValue();
            } else {
                // if first column of next row contains nothing it means the file is finished
                return null;
            }

            int eventType;
            if (charSeeker.seek(mark, columnDelimiters)) {
                eventType = charSeeker.extract(mark, extractors.int_()).intValue();
            } else {
                throw new GeneratorException("No event type found");
            }

            EventDecoder<BASE_EVENT_TYPE> decoder = decoders.get(eventType);
            if (null == decoder) {
                throw new NoSuchElementException(String.format(
                        "No decoder found that matches this column\nDECODER KEY: %s",
                        eventType
                ));
            }

            return decoder.decodeEvent(scheduledStartTime, charSeeker, extractors, columnDelimiters, mark);
        } catch (IOException e) {
            throw new GeneratorException("Error while retrieving next event", e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", getClass().getSimpleName()));
    }


    public static interface EventDecoder<BASE_EVENT_TYPE> {
        BASE_EVENT_TYPE decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark);
    }
}
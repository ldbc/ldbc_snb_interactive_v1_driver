package com.ldbc.driver.generator;


import com.ldbc.driver.util.csv.CharSeeker;
import com.ldbc.driver.util.csv.Extractors;
import com.ldbc.driver.util.csv.Mark;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;


public class CsvEventStreamReader_NEW_NEW<BASE_EVENT_TYPE> implements Iterator<BASE_EVENT_TYPE> {
    // TODO make Map<Integer, EventDecoder<BASE_EVENT_TYPE>> decoders
    private final Map<String, EventDecoder<BASE_EVENT_TYPE>> decoders;
    private final CharSeeker charSeeker;
    private final Mark mark;
    private final int[] delimiters;
    private BASE_EVENT_TYPE nextEvent = null;

    public CsvEventStreamReader_NEW_NEW(CharSeeker charSeeker,
                                        // TODO make Map<Integer, EventDecoder<BASE_EVENT_TYPE>> decoders
                                        Map<String, EventDecoder<BASE_EVENT_TYPE>> decoders,
                                        int[] delimiters) {
        this.charSeeker = charSeeker;
        this.mark = new Mark();
        this.delimiters = delimiters;
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
            if (charSeeker.seek(mark, delimiters)) {
                scheduledStartTime = charSeeker.extract(mark, Extractors.LONG);
            } else {
                // if first column of next row contains nothing it means the file is finished
                return null;
            }

            String eventType;
            if (charSeeker.seek(mark, delimiters)) {
                eventType = charSeeker.extract(mark, Extractors.STRING);
            } else {
                throw new GeneratorException("No event type found");
            }

            // TODO make Map<Integer, EventDecoder<BASE_EVENT_TYPE>> decoders
            EventDecoder<BASE_EVENT_TYPE> decoder = decoders.get(eventType);
            if (null == decoder) {
                throw new NoSuchElementException(String.format(
                        "No decoder found that matches this column\nDECODER KEY: %s",
                        eventType
                ));
            }

            return decoder.decodeEvent(scheduledStartTime, charSeeker, delimiters);
        } catch (IOException e) {
            throw new GeneratorException("Error while retrieving next event", e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", getClass().getSimpleName()));
    }


    public static interface EventDecoder<BASE_EVENT_TYPE> {
        BASE_EVENT_TYPE decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters);
    }
}
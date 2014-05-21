package com.ldbc.driver.generator;


import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.util.Function1;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Iterator;

public class CsvEventStreamReader<BASE_EVENT_TYPE> implements Iterator<BASE_EVENT_TYPE> {
    public static enum EventReturnPolicy {
        AT_LEAST_ONE_MATCH,
        EXACTLY_ONE_MATCH
    }
    public static class EventDescriptions<BASE_EVENT_TYPE> {
        private final Iterable<EventDecoder<BASE_EVENT_TYPE>> decoders;
        private final Function1<String[], BASE_EVENT_TYPE> decodeEventFun;

        public EventDescriptions(final Iterable<EventDecoder<BASE_EVENT_TYPE>> decoders, EventReturnPolicy eventReturnPolicy) {
            this.decoders = decoders;
            switch (eventReturnPolicy) {
                case AT_LEAST_ONE_MATCH:
                    decodeEventFun = new Function1<String[], BASE_EVENT_TYPE>() {
                        @Override
                        public BASE_EVENT_TYPE apply(String[] csvRow) {
                            for (EventDecoder<BASE_EVENT_TYPE> decoder : decoders) {
                                if (decoder.eventMatchesDecoder(csvRow)) return decoder.decodeEvent(csvRow);
                            }
                            throw new GeneratorException(String.format("Found no matching decoder for row\n%s", Arrays.toString(csvRow)));
                        }
                    };
                    break;
                case EXACTLY_ONE_MATCH:
                    decodeEventFun = new Function1<String[], BASE_EVENT_TYPE>() {
                        @Override
                        public BASE_EVENT_TYPE apply(String[] csvRow) {
                            EventDecoder<BASE_EVENT_TYPE> matchedDecoder = null;
                            for (EventDecoder<BASE_EVENT_TYPE> decoder : decoders) {
                                if (decoder.eventMatchesDecoder(csvRow)) {
                                    if (null != matchedDecoder)
                                        throw new GeneratorException(String.format("Found multiple matching decoders for row\n%s", Arrays.toString(csvRow)));
                                    matchedDecoder = decoder;
                                }
                            }
                            if (null == matchedDecoder)
                                throw new GeneratorException(String.format("Found no matching decoder for row\n%s", Arrays.toString(csvRow)));
                            return matchedDecoder.decodeEvent(csvRow);
                        }
                    };
                    break;
                default:
                    throw new GeneratorException(String.format("Unrecognized event return policy: %s", eventReturnPolicy));
            }
        }

        private BASE_EVENT_TYPE decodeEvent(String[] csvRow) {
            return decodeEventFun.apply(csvRow);
        }
    }

    public static interface EventDecoder<BASE_EVENT_TYPE> {
        boolean eventMatchesDecoder(String[] csvRow);

        BASE_EVENT_TYPE decodeEvent(String[] csvRow);
    }

    private final EventDescriptions<BASE_EVENT_TYPE> eventDescriptions;
    private final Iterator<String[]> csvFileReader;

    public CsvEventStreamReader(File csvFile, EventDescriptions<BASE_EVENT_TYPE> eventDescriptions) throws FileNotFoundException {
        this(csvFile, "\\|", eventDescriptions);
    }

    public CsvEventStreamReader(File csvFile, String separatorRegexString, EventDescriptions<BASE_EVENT_TYPE> eventDescriptions) throws FileNotFoundException {
        this.csvFileReader = new CsvFileReader(csvFile, separatorRegexString);
        this.eventDescriptions = eventDescriptions;
    }

    @Override
    public boolean hasNext() {
        return csvFileReader.hasNext();
    }

    @Override
    public BASE_EVENT_TYPE next() {
        String[] csvRow = csvFileReader.next();
        return eventDescriptions.decodeEvent(csvRow);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", getClass().getSimpleName()));
    }

}

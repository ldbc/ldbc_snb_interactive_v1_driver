package com.ldbc.driver.workloads.ldbc.socnet.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader;
import com.ldbc.driver.generator.GeneratorException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;

public class Query14EventStreamReader implements Iterator<Operation<?>> {
    public static final String PERSON_1_ID = "Person1ID";
    public static final String PERSON_2_ID = "Person2ID";
    public static final String PERSON_1_URI = "Person1URI";
    public static final String PERSON_2_URI = "Person2URI";

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final CsvEventStreamReader<Operation<?>> csvEventStreamReader;

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return true;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            String eventParamsAsJsonString = csvRow[0];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long person1Id = params.get(PERSON_1_ID).asLong();
            long person2Id = params.get(PERSON_2_ID).asLong();
            String person1Uri = params.get(PERSON_1_URI).asText();
            String person2Uri = params.get(PERSON_2_URI).asText();
            return new LdbcQuery14(person1Id, person1Uri, person2Id, person2Uri, LdbcQuery14.DEFAULT_LIMIT);
        }
    };

    public Query14EventStreamReader(Iterator<String[]> csvRowIterator) {
        this(csvRowIterator, CsvEventStreamReader.EventReturnPolicy.AT_LEAST_ONE_MATCH);
    }

    public Query14EventStreamReader(Iterator<String[]> csvRowIterator, CsvEventStreamReader.EventReturnPolicy eventReturnPolicy) {
        Iterable<CsvEventStreamReader.EventDecoder<Operation<?>>> decoders = Lists.newArrayList(EVENT_DECODER);
        CsvEventStreamReader.EventDescriptions<Operation<?>> eventDescriptions = new CsvEventStreamReader.EventDescriptions<>(decoders, eventReturnPolicy);
        this.csvEventStreamReader = new CsvEventStreamReader<>(csvRowIterator, eventDescriptions);
    }

    @Override
    public boolean hasNext() {
        return csvEventStreamReader.hasNext();
    }

    @Override
    public Operation<?> next() {
        return csvEventStreamReader.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", getClass().getSimpleName()));
    }
}

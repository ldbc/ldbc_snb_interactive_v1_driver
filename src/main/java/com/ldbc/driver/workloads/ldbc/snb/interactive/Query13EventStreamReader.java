package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader_OLD;
import com.ldbc.driver.generator.GeneratorException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;

public class Query13EventStreamReader implements Iterator<Operation<?>> {
    public static final String PERSON_1_ID = "Person1ID";
    public static final String PERSON_2_ID = "Person2ID";
    public static final String PERSON_1_URI = "Person1URI";
    public static final String PERSON_2_URI = "Person2URI";

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final CsvEventStreamReader_OLD<Operation<?>> csvEventStreamReaderOLD;

    public static final CsvEventStreamReader_OLD.EventDecoder<Operation<?>> EVENT_DECODER = new CsvEventStreamReader_OLD.EventDecoder<Operation<?>>() {
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
            return new LdbcQuery13(person1Id, person1Uri, person2Id, person2Uri);
        }
    };

    public Query13EventStreamReader(Iterator<String[]> csvRowIterator) {
        this(csvRowIterator, CsvEventStreamReader_OLD.EventReturnPolicy.AT_LEAST_ONE_MATCH);
    }

    public Query13EventStreamReader(Iterator<String[]> csvRowIterator, CsvEventStreamReader_OLD.EventReturnPolicy eventReturnPolicy) {
        Iterable<CsvEventStreamReader_OLD.EventDecoder<Operation<?>>> decoders = Lists.newArrayList(EVENT_DECODER);
        CsvEventStreamReader_OLD.EventDescriptions<Operation<?>> eventDescriptions = new CsvEventStreamReader_OLD.EventDescriptions<>(decoders, eventReturnPolicy);
        this.csvEventStreamReaderOLD = new CsvEventStreamReader_OLD<>(csvRowIterator, eventDescriptions);
    }

    @Override
    public boolean hasNext() {
        return csvEventStreamReaderOLD.hasNext();
    }

    @Override
    public Operation<?> next() {
        return csvEventStreamReaderOLD.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", getClass().getSimpleName()));
    }
}

package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader;
import com.ldbc.driver.generator.GeneratorException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;

public class Query4EventStreamReader implements Iterator<Operation<?>> {
    public static final String PERSON_ID = "PersonID";
    public static final String PERSON_URI = "PersonURI";
    public static final String DATE = "Date0";
    public static final String DURATION_DAYS = "Duration";

    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private static final SimpleDateFormat DATE_FORMAT;

    static {
        DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STRING);
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

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
            long personId = params.get(PERSON_ID).asLong();
            String personUri = params.get(PERSON_URI).asText();
            String dateString = params.get(DATE).asText();
            Date maxDate;
            try {
                maxDate = DATE_FORMAT.parse(dateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing date string\n%s", dateString), e);
            }
            int durationDays = params.get(DURATION_DAYS).asInt();
            return new LdbcQuery4(personId, personUri, maxDate, durationDays, LdbcQuery4.DEFAULT_LIMIT);
        }
    };

    public Query4EventStreamReader(Iterator<String[]> csvRowIterator) {
        this(csvRowIterator, CsvEventStreamReader.EventReturnPolicy.AT_LEAST_ONE_MATCH);
    }

    public Query4EventStreamReader(Iterator<String[]> csvRowIterator, CsvEventStreamReader.EventReturnPolicy eventReturnPolicy) {
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

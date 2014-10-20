package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader_OLD;
import com.ldbc.driver.generator.GeneratorException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;

public class Query3EventStreamReader implements Iterator<Operation<?>> {
    public static final String PERSON_ID = "PersonID";
    public static final String PERSON_URI = "PersonURI";
    public static final String DATE = "Date0";
    public static final String DURATION_DAYS = "Duration";
    public static final String COUNTRY_1 = "Country1";
    public static final String COUNTRY_2 = "Country2";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private static final SimpleDateFormat DATE_FORMAT;

    static {
        DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STRING);
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

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
            long personId = params.get(PERSON_ID).asLong();
            String personUri = params.get(PERSON_URI).asText();
            String dateString = params.get(DATE).asText();
            Date endDate;
            try {
                endDate = DATE_FORMAT.parse(dateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing date string\n%s", dateString), e);
            }
            int durationDays = params.get(DURATION_DAYS).asInt();
            String countryX = params.get(COUNTRY_1).asText();
            String countryY = params.get(COUNTRY_2).asText();
            return new LdbcQuery3(personId, personUri, countryX, countryY, endDate, durationDays, LdbcQuery3.DEFAULT_LIMIT);
        }
    };

    public Query3EventStreamReader(Iterator<String[]> csvRowIterator) {
        this(csvRowIterator, CsvEventStreamReader_OLD.EventReturnPolicy.AT_LEAST_ONE_MATCH);
    }

    public Query3EventStreamReader(Iterator<String[]> csvRowIterator, CsvEventStreamReader_OLD.EventReturnPolicy eventReturnPolicy) {
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

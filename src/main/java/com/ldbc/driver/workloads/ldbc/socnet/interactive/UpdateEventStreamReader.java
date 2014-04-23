package com.ldbc.driver.workloads.ldbc.socnet.interactive;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.temporal.Time;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class UpdateEventStreamReader implements Iterator<Operation<?>> {
    private final static String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STRING);

    private final static String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private final static SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);

    private enum UpdateEventType {
        ADD_PERSON,
        ADD_POST_LIKE,
        ADD_COMMENT_LIKE,
        ADD_FORUM,
        ADD_FORUM_MEMBERSHIP,
        ADD_POST,
        ADD_COMMENT,
        ADD_FRIENDSHIP
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Iterator<String[]> csvFileReader;

    public UpdateEventStreamReader(Iterator<String[]> csvFileReader) {
        this.csvFileReader = csvFileReader;
    }

    @Override
    public boolean hasNext() {
        return csvFileReader.hasNext();
    }

    @Override
    public Operation<?> next() {
        String[] csvRow = csvFileReader.next();
        Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
        UpdateEventType eventType = UpdateEventType.valueOf(csvRow[1]);
        String eventParamsAsJsonString = csvRow[2];
        try {
            return buildOperation(eventType, eventDueTime, eventParamsAsJsonString);
        } catch (Throwable e) {
            throw new GeneratorException(String.format("Unable to parse update operation\n%s", Arrays.toString(csvRow)));
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", UpdateEventStreamReader.class.getSimpleName()));
    }

    Operation<?> buildOperation(UpdateEventType eventType, Time eventDueTime, String eventParamsAsJsonString) throws IOException, ParseException {
        switch (eventType) {
            case ADD_PERSON:
                LdbcUpdate1AddPerson operation = parseAddPerson(eventParamsAsJsonString);
                operation.setScheduledStartTime(eventDueTime);
                return operation;
            case ADD_POST_LIKE:
                // TODO
                return null;
            case ADD_COMMENT_LIKE:
                // TODO
                return null;
            case ADD_FORUM:
                // TODO
                return null;
            case ADD_FORUM_MEMBERSHIP:
                // TODO
                return null;
            case ADD_POST:
                // TODO
                return null;
            case ADD_COMMENT:
                // TODO
                return null;
            case ADD_FRIENDSHIP:
                // TODO
                return null;
            default:
                // TODO
                return null;
        }
    }

    LdbcUpdate1AddPerson parseAddPerson(String eventParamsAsJsonString) throws IOException, ParseException {
        JsonNode params = objectMapper.readTree(eventParamsAsJsonString);
        long personId = params.get(0).asLong();
        String firstName = params.get(1).asText();
        String lastName = params.get(2).asText();
        String gender = params.get(3).asText();
        String birthdayString = params.get(4).asText();
        Date birthday = DATE_FORMAT.parse(birthdayString);
        String creationDateString = params.get(5).asText();
        Date creationDate = DATE_FORMAT.parse(creationDateString);
        String locationIp = params.get(6).asText();
        String browserUsed = params.get(7).asText();
        long cityId = params.get(8).asLong();
        List<String> languages = Lists.newArrayList(Iterables.transform(params.get(9), new Function<JsonNode, String>() {
            @Override
            public String apply(JsonNode input) {
                return input.asText();
            }
        }));
        List<String> emails = Lists.newArrayList(Iterables.transform(params.get(10), new Function<JsonNode, String>() {
            @Override
            public String apply(JsonNode input) {
                return input.asText();
            }
        }));
        List<Long> tagIds = Lists.newArrayList(Iterables.transform(params.get(11), new Function<JsonNode, Long>() {
            @Override
            public Long apply(JsonNode input) {
                return input.asLong();
            }
        }));
        List<LdbcUpdate1AddPerson.Organization> studyAt = Lists.newArrayList(Iterables.transform(params.get(12), new Function<JsonNode, LdbcUpdate1AddPerson.Organization>() {
            @Override
            public LdbcUpdate1AddPerson.Organization apply(JsonNode input) {
                return new LdbcUpdate1AddPerson.Organization(input.get(0).asLong(), input.get(1).asInt());
            }
        }));
        List<LdbcUpdate1AddPerson.Organization> workAt = Lists.newArrayList(Iterables.transform(params.get(13), new Function<JsonNode, LdbcUpdate1AddPerson.Organization>() {
            @Override
            public LdbcUpdate1AddPerson.Organization apply(JsonNode input) {
                return new LdbcUpdate1AddPerson.Organization(input.get(0).asLong(), input.get(1).asInt());
            }
        }));
        return new LdbcUpdate1AddPerson(
                personId,
                firstName,
                lastName,
                gender,
                birthday,
                creationDate,
                locationIp,
                browserUsed,
                cityId,
                languages.toArray(new String[languages.size()]),
                emails.toArray(new String[emails.size()]),
                longListToPrimitiveLongArray(tagIds),
                studyAt.toArray(new LdbcUpdate1AddPerson.Organization[studyAt.size()]),
                workAt.toArray(new LdbcUpdate1AddPerson.Organization[workAt.size()]));
    }

    LdbcUpdate2AddPostLike parseAddPostLike(String eventParamsAsJsonString) throws IOException, ParseException {
        JsonNode params = objectMapper.readTree(eventParamsAsJsonString);
        long personId = params.get(0).asLong();
        long postId = params.get(1).asLong();
        String creationDateString = params.get(2).asText();
        Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
        return new LdbcUpdate2AddPostLike(personId, postId, creationDate);
    }

    LdbcUpdate3AddCommentLike parseAddCommentLike(String eventParamsAsJsonString) throws IOException, ParseException {
        JsonNode params = objectMapper.readTree(eventParamsAsJsonString);
        long personId = params.get(0).asLong();
        long commentId = params.get(1).asLong();
        String creationDateString = params.get(2).asText();
        Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
        return new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
    }

    LdbcUpdate4AddForum parseAddForum(String eventParamsAsJsonString) throws IOException, ParseException {
        JsonNode params = objectMapper.readTree(eventParamsAsJsonString);
        long forumId = params.get(0).asLong();
        String forumTitle = params.get(1).asText();
        String creationDateString = params.get(2).asText();
        Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
        long moderatorPersonId = params.get(3).asLong();
        List<Long> tagIdsList = Lists.newArrayList(Iterables.transform(params.get(4), new Function<JsonNode, Long>() {
            @Override
            public Long apply(JsonNode input) {
                return input.asLong();
            }
        }));
        return new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, longListToPrimitiveLongArray(tagIdsList));
    }

    LdbcUpdate5AddForumMembership parseAddForumMembership(String eventParamsAsJsonString) throws IOException, ParseException {
        JsonNode params = objectMapper.readTree(eventParamsAsJsonString);
        long forumId = params.get(0).asLong();
        long personId = params.get(1).asLong();
        String creationDateString = params.get(2).asText();
        Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
        return new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
    }

    LdbcUpdate6AddPost parseAddPost(String eventParamsAsJsonString) throws IOException, ParseException {
        JsonNode params = objectMapper.readTree(eventParamsAsJsonString);
        long postId = params.get(0).asLong();
        String imageFile = params.get(1).asText();
        String creationDateString = params.get(2).asText();
        Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
        String locationIp = params.get(3).asText();
        String browserUsed = params.get(4).asText();
        String language = params.get(5).asText();
        String content = params.get(6).asText();
        int length = params.get(7).asInt();
        long authorPersonId = params.get(8).asLong();
        long forumId = params.get(9).asLong();
        long countryId = params.get(10).asLong();
        List<Long> tagIdsList = Lists.newArrayList(Iterables.transform(params.get(11), new Function<JsonNode, Long>() {
            @Override
            public Long apply(JsonNode input) {
                return input.asLong();
            }
        }));
        return new LdbcUpdate6AddPost(
                postId,
                imageFile,
                creationDate,
                locationIp,
                browserUsed,
                language,
                content,
                length,
                authorPersonId,
                forumId,
                countryId,
                longListToPrimitiveLongArray(tagIdsList));
    }

    long[] longListToPrimitiveLongArray(List<Long> longList) {
        long[] longArray = new long[longList.size()];
        for (int i = 0; i < longList.size(); i++)
            longArray[i] = longList.get(i);
        return longArray;
    }
}

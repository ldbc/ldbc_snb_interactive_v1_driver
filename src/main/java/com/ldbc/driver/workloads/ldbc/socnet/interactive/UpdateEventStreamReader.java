package com.ldbc.driver.workloads.ldbc.socnet.interactive;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.temporal.Time;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class UpdateEventStreamReader implements Iterator<Operation<?>> {
    private final static String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STRING);
    private final static String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private final static SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);

    private enum UpdateEventType {
        ADD_PERSON,
        ADD_LIKE_POST,
        ADD_LIKE_COMMENT,
        ADD_FORUM,
        ADD_FORUM_MEMBERSHIP,
        ADD_POST,
        ADD_COMMENT,
        ADD_FRIENDSHIP
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Iterator<String[]> csvFileReader;

    public UpdateEventStreamReader(File csvFile) throws FileNotFoundException {
        this.csvFileReader = new CsvFileReader(csvFile, "\\|");
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
            throw new GeneratorException(String.format("Unable to parse update operation\n%s", Arrays.toString(csvRow)), e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", UpdateEventStreamReader.class.getSimpleName()));
    }

    Operation<?> buildOperation(UpdateEventType eventType, Time eventDueTime, String eventParamsAsJsonString) throws IOException, ParseException {
        switch (eventType) {
            case ADD_PERSON:
                LdbcUpdate1AddPerson addPerson = parseAddPerson(eventParamsAsJsonString);
                addPerson.setScheduledStartTime(eventDueTime);
                return addPerson;
            case ADD_LIKE_POST:
                LdbcUpdate2AddPostLike addPostLike = parseAddPostLike(eventParamsAsJsonString);
                addPostLike.setScheduledStartTime(eventDueTime);
                return addPostLike;
            case ADD_LIKE_COMMENT:
                LdbcUpdate3AddCommentLike addCommentLike = parseAddCommentLike(eventParamsAsJsonString);
                addCommentLike.setScheduledStartTime(eventDueTime);
                return addCommentLike;
            case ADD_FORUM:
                LdbcUpdate4AddForum addForum = parseAddForum(eventParamsAsJsonString);
                addForum.setScheduledStartTime(eventDueTime);
                return addForum;
            case ADD_FORUM_MEMBERSHIP:
                LdbcUpdate5AddForumMembership addForumMembership = parseAddForumMembership(eventParamsAsJsonString);
                addForumMembership.setScheduledStartTime(eventDueTime);
                return addForumMembership;
            case ADD_POST:
                LdbcUpdate6AddPost addPost = parseAddPost(eventParamsAsJsonString);
                addPost.setScheduledStartTime(eventDueTime);
                return addPost;
            case ADD_COMMENT:
                LdbcUpdate7AddComment addComment = parseAddComment(eventParamsAsJsonString);
                addComment.setScheduledStartTime(eventDueTime);
                return addComment;
            case ADD_FRIENDSHIP:
                LdbcUpdate8AddFriendship addFriendship = parseAddFriendship(eventParamsAsJsonString);
                addFriendship.setScheduledStartTime(eventDueTime);
                return addFriendship;
            default:
                throw new RuntimeException(String.format("Unknown event type: %s", eventType.name()));
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
        Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
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

    LdbcUpdate7AddComment parseAddComment(String eventParamsAsJsonString) throws IOException, ParseException {
        JsonNode params = objectMapper.readTree(eventParamsAsJsonString);
        long commentId = params.get(0).asLong();
        String creationDateString = params.get(1).asText();
        Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
        String locationIp = params.get(2).asText();
        String browserUsed = params.get(3).asText();
        String content = params.get(4).asText();
        int length = params.get(5).asInt();
        long authorPersonId = params.get(6).asLong();
        long countryId = params.get(7).asLong();
        long replyOfPostId = params.get(8).asLong();
        long replyOfCommentId = params.get(9).asLong();
        List<Long> tagIdsList = Lists.newArrayList(Iterables.transform(params.get(10), new Function<JsonNode, Long>() {
            @Override
            public Long apply(JsonNode input) {
                return input.asLong();
            }
        }));
        return new LdbcUpdate7AddComment(
                commentId,
                creationDate,
                locationIp,
                browserUsed,
                content,
                length,
                authorPersonId,
                countryId,
                replyOfPostId,
                replyOfCommentId,
                longListToPrimitiveLongArray(tagIdsList));
    }

    LdbcUpdate8AddFriendship parseAddFriendship(String eventParamsAsJsonString) throws IOException, ParseException {
        JsonNode params = objectMapper.readTree(eventParamsAsJsonString);
        long person1Id = params.get(0).asLong();
        long person2Id = params.get(1).asLong();
        String creationDateString = params.get(2).asText();
        Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
        return new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
    }

    long[] longListToPrimitiveLongArray(List<Long> longList) {
        long[] longArray = new long[longList.size()];
        for (int i = 0; i < longList.size(); i++)
            longArray[i] = longList.get(i);
        return longArray;
    }

    public class CsvFileReader implements Iterator<String[]> {
        private final Pattern columnSeparatorPattern;
        private final BufferedReader csvReader;

        private String[] next = null;
        private boolean closed = false;

        public CsvFileReader(File csvFile, String regexSeparator) throws FileNotFoundException {
            this.csvReader = new BufferedReader(new FileReader(csvFile));
            this.columnSeparatorPattern = Pattern.compile(regexSeparator);
        }

        @Override
        public boolean hasNext() {
            if (closed) return false;
            next = (next == null) ? nextLine() : next;
            if (null == next) closed = closeReader();
            return (null != next);
        }

        @Override
        public String[] next() {
            next = (null == next) ? nextLine() : next;
            if (null == next) throw new NoSuchElementException("No more lines to read");
            String[] tempNext = next;
            next = null;
            return tempNext;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private String[] nextLine() {
            String csvLine;
            try {
                csvLine = csvReader.readLine();
                if (null == csvLine) return null;
                return parseLine(csvLine);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Error retrieving next csv entry from file [%s]", csvReader), e);
            }
        }

        private String[] parseLine(String csvLine) {
            return columnSeparatorPattern.split(csvLine, -1);
        }

        private boolean closeReader() {
            if (closed) {
                throw new RuntimeException("Can not close file multiple times");
            }
            if (null == csvReader) {
                throw new RuntimeException("Can not close file - reader is null");
            }
            try {
                csvReader.close();
            } catch (IOException e) {
                throw new RuntimeException(String.format("Error closing file [%s]", csvReader), e);
            }
            return true;
        }
    }
}

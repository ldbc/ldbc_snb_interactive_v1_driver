package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.temporal.Time;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WriteEventStreamReader implements Iterator<Operation<?>> {

    public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    public static final String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final CsvEventStreamReader<Operation<?>> csvEventStreamReader;

    private final CsvEventStreamReader.EventDecoder<Operation<?>> addPersonDecoder;
    private final CsvEventStreamReader.EventDecoder<Operation<?>> addLikePostDecoder;
    private final CsvEventStreamReader.EventDecoder<Operation<?>> addLikeCommentDecoder;
    private final CsvEventStreamReader.EventDecoder<Operation<?>> addForumDecoder;
    private final CsvEventStreamReader.EventDecoder<Operation<?>> addForumMembershipDecoder;
    private final CsvEventStreamReader.EventDecoder<Operation<?>> addPostDecoder;
    private final CsvEventStreamReader.EventDecoder<Operation<?>> addCommentDecoder;
    private final CsvEventStreamReader.EventDecoder<Operation<?>> addFriendshipDecoder;

    public WriteEventStreamReader(Iterator<String[]> csvRowIterator, CsvEventStreamReader.EventReturnPolicy eventReturnPolicy) {
        List<CsvEventStreamReader.EventDecoder<Operation<?>>> decoders = new ArrayList<>();
        {
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            this.addPersonDecoder = new EventDecoderAddPerson(dateFormat, dateTimeFormat);
            decoders.add(this.addPersonDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            this.addLikePostDecoder = new EventDecoderAddLikePost(dateTimeFormat);
            decoders.add(this.addLikePostDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            this.addLikeCommentDecoder = new EventDecoderAddLikeComment(dateTimeFormat);
            decoders.add(this.addLikeCommentDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            this.addForumDecoder = new EventDecoderAddForum(dateTimeFormat);
            decoders.add(this.addForumDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            this.addForumMembershipDecoder = new EventDecoderAddForumMembership(dateTimeFormat);
            decoders.add(this.addForumMembershipDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            this.addPostDecoder = new EventDecoderAddPost(dateTimeFormat);
            decoders.add(this.addPostDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            this.addCommentDecoder = new EventDecoderAddComment(dateTimeFormat);
            decoders.add(this.addCommentDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            this.addFriendshipDecoder = new EventDecoderAddFriendship(dateTimeFormat);
            decoders.add(this.addFriendshipDecoder);
        }
        CsvEventStreamReader.EventDescriptions<Operation<?>> eventDescriptions = new CsvEventStreamReader.EventDescriptions<>(decoders, eventReturnPolicy);
        this.csvEventStreamReader = new CsvEventStreamReader<>(csvRowIterator, eventDescriptions);
    }

    public CsvEventStreamReader.EventDecoder<Operation<?>> addPersonDecoder() {
        return addPersonDecoder;
    }

    public CsvEventStreamReader.EventDecoder<Operation<?>> addLikePostDecoder() {
        return addLikePostDecoder;
    }

    public CsvEventStreamReader.EventDecoder<Operation<?>> addLikeCommentDecoder() {
        return addLikeCommentDecoder;
    }

    public CsvEventStreamReader.EventDecoder<Operation<?>> addForumDecoder() {
        return addForumDecoder;
    }

    public CsvEventStreamReader.EventDecoder<Operation<?>> addForumMembershipDecoder() {
        return addForumMembershipDecoder;
    }

    public CsvEventStreamReader.EventDecoder<Operation<?>> addPostDecoder() {
        return addPostDecoder;
    }

    public CsvEventStreamReader.EventDecoder<Operation<?>> addCommentDecoder() {
        return addCommentDecoder;
    }

    public CsvEventStreamReader.EventDecoder<Operation<?>> addFriendshipDecoder() {
        return addFriendshipDecoder;
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

    public static class EventDecoderAddPerson implements CsvEventStreamReader.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateFormat;
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddPerson(SimpleDateFormat dateFormat, SimpleDateFormat dateTimeFormat) {
            this.dateFormat = dateFormat;
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_PERSON");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long personId = params.get(0).asLong();
            String firstName = params.get(1).asText();
            String lastName = params.get(2).asText();
            String gender = params.get(3).asText();
            String birthdayString = params.get(4).asText();
            Date birthday;
            try {
                birthday = dateFormat.parse(birthdayString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing birthday string\n%s", birthdayString), e);
            }
            String creationDateString = params.get(5).asText();
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", birthdayString), e);
            }
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
            Operation<?> operation = new LdbcUpdate1AddPerson(
                    personId,
                    firstName,
                    lastName,
                    gender,
                    birthday,
                    creationDate,
                    locationIp,
                    browserUsed,
                    cityId,
                    languages,
                    emails,
                    tagIds,
                    studyAt,
                    workAt);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddLikePost implements CsvEventStreamReader.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddLikePost(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_LIKE_POST");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long personId = params.get(0).asLong();
            long postId = params.get(1).asLong();
            String creationDateString = params.get(2).asText();
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate2AddPostLike(personId, postId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddLikeComment implements CsvEventStreamReader.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddLikeComment(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_LIKE_COMMENT");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long personId = params.get(0).asLong();
            long commentId = params.get(1).asLong();
            String creationDateString = params.get(2).asText();
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddForum implements CsvEventStreamReader.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddForum(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_FORUM");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long forumId = params.get(0).asLong();
            String forumTitle = params.get(1).asText();
            String creationDateString = params.get(2).asText();
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            long moderatorPersonId = params.get(3).asLong();
            List<Long> tagIdsList = Lists.newArrayList(Iterables.transform(params.get(4), new Function<JsonNode, Long>() {
                @Override
                public Long apply(JsonNode input) {
                    return input.asLong();
                }
            }));
            Operation<?> operation = new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIdsList);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddForumMembership implements CsvEventStreamReader.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddForumMembership(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_FORUM_MEMBERSHIP");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long forumId = params.get(0).asLong();
            long personId = params.get(1).asLong();
            String creationDateString = params.get(2).asText();
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddPost implements CsvEventStreamReader.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddPost(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_POST");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long postId = params.get(0).asLong();
            String imageFile = params.get(1).asText();
            String creationDateString = params.get(2).asText();
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
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
            Operation<?> operation = new LdbcUpdate6AddPost(
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
                    tagIdsList);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddComment implements CsvEventStreamReader.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddComment(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_COMMENT");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long commentId = params.get(0).asLong();
            String creationDateString = params.get(1).asText();
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
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
            Operation<?> operation = new LdbcUpdate7AddComment(
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
                    tagIdsList);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddFriendship implements CsvEventStreamReader.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddFriendship(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_FRIENDSHIP");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long person1Id = params.get(0).asLong();
            long person2Id = params.get(1).asLong();
            String creationDateString = params.get(2).asText();
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }
}

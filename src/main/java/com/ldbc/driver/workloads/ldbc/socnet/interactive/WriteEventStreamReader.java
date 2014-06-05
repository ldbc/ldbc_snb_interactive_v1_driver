package com.ldbc.driver.workloads.ldbc.socnet.interactive;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.CsvFileReader;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class WriteEventStreamReader implements Iterator<Operation<?>> {

    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STRING);
    private static final String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final CsvEventStreamReader<Operation<?>> csvEventStreamReader;

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER_ADD_PERSON = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
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
                birthday = DATE_FORMAT.parse(birthdayString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing birthday string\n%s", birthdayString), e);
            }
            String creationDateString = params.get(5).asText();
            Date creationDate;
            try {
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
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
                    languages.toArray(new String[languages.size()]),
                    emails.toArray(new String[emails.size()]),
                    longListToPrimitiveLongArray(tagIds),
                    studyAt.toArray(new LdbcUpdate1AddPerson.Organization[studyAt.size()]),
                    workAt.toArray(new LdbcUpdate1AddPerson.Organization[workAt.size()]));
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER_ADD_LIKE_POST = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
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
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate2AddPostLike(personId, postId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER_ADD_LIKE_COMMENT = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
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
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER_ADD_FORUM = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
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
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
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
            Operation<?> operation = new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, longListToPrimitiveLongArray(tagIdsList));
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER_ADD_FORUM_MEMBERSHIP = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
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
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER_ADD_POST = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
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
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
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
                    longListToPrimitiveLongArray(tagIdsList));
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER_ADD_COMMENT = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
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
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
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
                    longListToPrimitiveLongArray(tagIdsList));
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    public static final CsvEventStreamReader.EventDecoder<Operation<?>> EVENT_DECODER_ADD_FRIENDSHIP = new CsvEventStreamReader.EventDecoder<Operation<?>>() {
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
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    public WriteEventStreamReader(File csvFile) throws FileNotFoundException {
        this(csvFile, CsvEventStreamReader.EventReturnPolicy.EXACTLY_ONE_MATCH);
    }

    public WriteEventStreamReader(File csvFile, CsvEventStreamReader.EventReturnPolicy eventReturnPolicy) throws FileNotFoundException {
        this(csvFile, "\\|", eventReturnPolicy);
    }

    public WriteEventStreamReader(File csvFile, String separatorRegexString, CsvEventStreamReader.EventReturnPolicy eventReturnPolicy) throws FileNotFoundException {
        Iterable<CsvEventStreamReader.EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                EVENT_DECODER_ADD_PERSON,
                EVENT_DECODER_ADD_LIKE_POST,
                EVENT_DECODER_ADD_LIKE_COMMENT,
                EVENT_DECODER_ADD_FORUM,
                EVENT_DECODER_ADD_FORUM_MEMBERSHIP,
                EVENT_DECODER_ADD_POST,
                EVENT_DECODER_ADD_COMMENT,
                EVENT_DECODER_ADD_FRIENDSHIP);
        CsvEventStreamReader.EventDescriptions<Operation<?>> eventDescriptions = new CsvEventStreamReader.EventDescriptions<>(decoders, eventReturnPolicy);
        this.csvEventStreamReader = new CsvEventStreamReader<>(new CsvFileReader(csvFile, separatorRegexString), eventDescriptions);
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

    static long[] longListToPrimitiveLongArray(List<Long> longList) {
        long[] longArray = new long[longList.size()];
        for (int i = 0; i < longList.size(); i++)
            longArray[i] = longList.get(i);
        return longArray;
    }
}

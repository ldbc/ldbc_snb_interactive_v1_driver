package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader_NEW;
import com.ldbc.driver.generator.CsvEventStreamReader_NEW.EventDecoder;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function1;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class WriteEventStreamReaderRegex implements Iterator<Operation<?>> {
    public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    public static final String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static final List<String> EMPTY_LIST = new ArrayList<>();

    private final CsvEventStreamReader_NEW<Operation<?>, String> csvEventStreamReader;

    public WriteEventStreamReaderRegex(Iterator<String[]> csvRowIterator) {
        Map<String, EventDecoder<Operation<?>>> decoders = new HashMap<>();
        {
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addPersonDecoder = new EventDecoderAddPerson(dateFormat, dateTimeFormat);
            decoders.put("1", addPersonDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addLikePostDecoder = new EventDecoderAddLikePost(dateTimeFormat);
            decoders.put("2", addLikePostDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addLikeCommentDecoder = new EventDecoderAddLikeComment(dateTimeFormat);
            decoders.put("3", addLikeCommentDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addForumDecoder = new EventDecoderAddForum(dateTimeFormat);
            decoders.put("4", addForumDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addForumMembershipDecoder = new EventDecoderAddForumMembership(dateTimeFormat);
            decoders.put("5", addForumMembershipDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addPostDecoder = new EventDecoderAddPost(dateTimeFormat);
            decoders.put("6", addPostDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addCommentDecoder = new EventDecoderAddComment(dateTimeFormat);
            decoders.put("7", addCommentDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addFriendshipDecoder = new EventDecoderAddFriendship(dateTimeFormat);
            decoders.put("8", addFriendshipDecoder);
        }
        Function1<String[], String> decoderKeyExtractor = new Function1<String[], String>() {
            @Override
            public String apply(String[] csvRow) {
                return csvRow[1];
            }
        };
        this.csvEventStreamReader = new CsvEventStreamReader_NEW<>(csvRowIterator, decoders, decoderKeyExtractor);
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

    public static class EventDecoderAddPerson implements CsvEventStreamReader_NEW.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateFormat;
        private final SimpleDateFormat dateTimeFormat;
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");
        private final Pattern tupleSeparatorPattern = Pattern.compile(",");

        public EventDecoderAddPerson(SimpleDateFormat dateFormat, SimpleDateFormat dateTimeFormat) {
            this.dateFormat = dateFormat;
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long personId = Long.parseLong(csvRow[2]);

            String firstName = csvRow[3];

            String lastName = csvRow[4];

            String gender = csvRow[5];

            String birthdayString = csvRow[6];
            Date birthday;
            try {
                birthday = dateFormat.parse(birthdayString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing birthday string\n%s", birthdayString), e);
            }

            String creationDateString = csvRow[7];
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", birthdayString), e);
            }

            String locationIp = csvRow[8];

            String browserUsed = csvRow[9];

            long cityId = Long.parseLong(csvRow[10]);

            String languagesString = csvRow[11];
            List<String> languages = (languagesString.isEmpty())
                    ? EMPTY_LIST
                    : Lists.newArrayList(collectionSeparatorPattern.split(languagesString, -1));

            String emailsString = csvRow[12];
            List<String> emails = (emailsString.isEmpty())
                    ? EMPTY_LIST
                    : Lists.newArrayList(collectionSeparatorPattern.split(emailsString, -1));

            String tagIdsAsString = csvRow[13];
            List<Long> tagIds = new ArrayList<>();
            if (false == tagIdsAsString.isEmpty()) {
                String[] tagIdsAsStrings = collectionSeparatorPattern.split(tagIdsAsString, -1);
                for (String tagId : tagIdsAsStrings) {
                    tagIds.add(Long.parseLong(tagId));
                }
            }

            String studyAtsAsString = csvRow[14];
            List<LdbcUpdate1AddPerson.Organization> studyAts = new ArrayList<>();
            if (false == studyAtsAsString.isEmpty()) {
                String[] studyAtsAsStrings = collectionSeparatorPattern.split(studyAtsAsString, -1);
                for (String studyAtAsString : studyAtsAsStrings) {
                    String[] studyAtAsStringArray = tupleSeparatorPattern.split(studyAtAsString, -1);
                    studyAts.add(new LdbcUpdate1AddPerson.Organization(
                                    Long.parseLong(studyAtAsStringArray[0]),
                                    Integer.parseInt(studyAtAsStringArray[1])
                            )
                    );
                }
            }

            String worksAtAsString = csvRow[15];
            List<LdbcUpdate1AddPerson.Organization> workAts = new ArrayList<>();
            if (false == worksAtAsString.isEmpty()) {
                String[] workAtsAsStrings = collectionSeparatorPattern.split(worksAtAsString, -1);
                for (String workAtAsString : workAtsAsStrings) {
                    String[] workAtAsStringArray = tupleSeparatorPattern.split(workAtAsString, -1);
                    workAts.add(new LdbcUpdate1AddPerson.Organization(
                                    Long.parseLong(workAtAsStringArray[0]),
                                    Integer.parseInt(workAtAsStringArray[1])
                            )
                    );
                }
            }

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
                    studyAts,
                    workAts);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddLikePost implements CsvEventStreamReader_NEW.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddLikePost(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long personId = Long.parseLong(csvRow[2]);

            long postId = Long.parseLong(csvRow[3]);

            String creationDateString = csvRow[4];
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

    public static class EventDecoderAddLikeComment implements CsvEventStreamReader_NEW.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddLikeComment(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long personId = Long.parseLong(csvRow[2]);

            long commentId = Long.parseLong(csvRow[3]);

            String creationDateString = csvRow[4];
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

    public static class EventDecoderAddForum implements CsvEventStreamReader_NEW.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");

        public EventDecoderAddForum(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long forumId = Long.parseLong(csvRow[2]);

            String forumTitle = csvRow[3];

            String creationDateString = csvRow[4];

            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }

            long moderatorPersonId = Long.parseLong(csvRow[5]);

            String tagIdsAsString = csvRow[6];
            List<Long> tagIds = new ArrayList<>();
            if (false == tagIdsAsString.isEmpty()) {
                String[] tagIdsAsStrings = collectionSeparatorPattern.split(tagIdsAsString, -1);
                for (String tagId : tagIdsAsStrings) {
                    tagIds.add(Long.parseLong(tagId));
                }
            }

            Operation<?> operation = new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIds);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddForumMembership implements CsvEventStreamReader_NEW.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddForumMembership(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long forumId = Long.parseLong(csvRow[2]);

            long personId = Long.parseLong(csvRow[3]);

            String creationDateString = csvRow[4];
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

    public static class EventDecoderAddPost implements CsvEventStreamReader_NEW.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");

        public EventDecoderAddPost(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long postId = Long.parseLong(csvRow[2]);

            String imageFile = csvRow[3];

            String creationDateString = csvRow[4];
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }

            String locationIp = csvRow[5];

            String browserUsed = csvRow[6];

            String language = csvRow[7];

            String content = csvRow[8];

            int length = Integer.parseInt(csvRow[9]);

            long authorPersonId = Long.parseLong(csvRow[10]);

            long forumId = Long.parseLong(csvRow[11]);

            long countryId = Long.parseLong(csvRow[12]);

            String tagIdsAsString = csvRow[13];
            List<Long> tagIds = new ArrayList<>();
            if (false == tagIdsAsString.isEmpty()) {
                String[] tagIdsAsStrings = collectionSeparatorPattern.split(tagIdsAsString, -1);
                for (String tagId : tagIdsAsStrings) {
                    tagIds.add(Long.parseLong(tagId));
                }
            }

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
                    tagIds);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddComment implements CsvEventStreamReader_NEW.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");

        public EventDecoderAddComment(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long commentId = Long.parseLong(csvRow[2]);

            String creationDateString = csvRow[3];
            Date creationDate;
            try {
                creationDate = dateTimeFormat.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }

            String locationIp = csvRow[4];

            String browserUsed = csvRow[5];

            String content = csvRow[6];

            int length = Integer.parseInt(csvRow[7]);

            long authorPersonId = Long.parseLong(csvRow[8]);

            long countryId = Long.parseLong(csvRow[9]);

            long replyOfPostId = Long.parseLong(csvRow[10]);

            long replyOfCommentId = Long.parseLong(csvRow[11]);

            String tagIdsAsString = csvRow[12];
            List<Long> tagIds = new ArrayList<>();
            if (false == tagIdsAsString.isEmpty()) {
                String[] tagIdsAsStrings = collectionSeparatorPattern.split(tagIdsAsString, -1);
                for (String tagId : tagIdsAsStrings) {
                    tagIds.add(Long.parseLong(tagId));
                }
            }

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
                    tagIds);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddFriendship implements CsvEventStreamReader_NEW.EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;

        public EventDecoderAddFriendship(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long person1Id = Long.parseLong(csvRow[2]);

            long person2Id = Long.parseLong(csvRow[3]);

            String creationDateString = csvRow[4];
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

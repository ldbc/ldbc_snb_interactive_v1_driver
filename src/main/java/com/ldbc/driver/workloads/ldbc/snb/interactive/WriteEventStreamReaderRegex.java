package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReaderCsvReader;
import com.ldbc.driver.generator.CsvEventStreamReaderCsvReader.EventDecoder;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function1;

import java.util.*;
import java.util.regex.Pattern;

public class WriteEventStreamReaderRegex implements Iterator<Operation<?>> {
    private static final List<String> EMPTY_LIST = new ArrayList<>();
    private final CsvEventStreamReaderCsvReader<Operation<?>, String> csvEventStreamReader;

    public WriteEventStreamReaderRegex(Iterator<String[]> csvRowIterator) {
        Map<String, EventDecoder<Operation<?>>> decoders = new HashMap<>();
        decoders.put("1", new EventDecoderAddPerson());
        decoders.put("2", new EventDecoderAddLikePost());
        decoders.put("3", new EventDecoderAddLikeComment());
        decoders.put("4", new EventDecoderAddForum());
        decoders.put("5", new EventDecoderAddForumMembership());
        decoders.put("6", new EventDecoderAddPost());
        decoders.put("7", new EventDecoderAddComment());
        decoders.put("8", new EventDecoderAddFriendship());
        Function1<String[], String> decoderKeyExtractor = new Function1<String[], String>() {
            @Override
            public String apply(String[] csvRow) {
                return csvRow[1];
            }
        };
        this.csvEventStreamReader = new CsvEventStreamReaderCsvReader<>(csvRowIterator, decoders, decoderKeyExtractor);
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

    public static class EventDecoderAddPerson implements EventDecoder<Operation<?>> {
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");
        private final Pattern tupleSeparatorPattern = Pattern.compile(",");

        public EventDecoderAddPerson() {
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long personId = Long.parseLong(csvRow[2]);

            String firstName = csvRow[3];

            String lastName = csvRow[4];

            String gender = csvRow[5];

            String birthdayString = csvRow[6];
            Date birthday = new Date(Long.parseLong(birthdayString));

            String creationDateString = csvRow[7];
            Date creationDate = new Date(Long.parseLong(creationDateString));

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

    public static class EventDecoderAddLikePost implements EventDecoder<Operation<?>> {
        public EventDecoderAddLikePost() {
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long personId = Long.parseLong(csvRow[2]);

            long postId = Long.parseLong(csvRow[3]);

            String creationDateString = csvRow[4];
            Date creationDate = new Date(Long.parseLong(creationDateString));

            Operation<?> operation = new LdbcUpdate2AddPostLike(personId, postId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddLikeComment implements EventDecoder<Operation<?>> {
        public EventDecoderAddLikeComment() {
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long personId = Long.parseLong(csvRow[2]);

            long commentId = Long.parseLong(csvRow[3]);

            String creationDateString = csvRow[4];
            Date creationDate = new Date(Long.parseLong(creationDateString));

            Operation<?> operation = new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddForum implements EventDecoder<Operation<?>> {
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");

        public EventDecoderAddForum() {
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long forumId = Long.parseLong(csvRow[2]);

            String forumTitle = csvRow[3];

            String creationDateString = csvRow[4];
            Date creationDate = new Date(Long.parseLong(creationDateString));

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

    public static class EventDecoderAddForumMembership implements EventDecoder<Operation<?>> {
        public EventDecoderAddForumMembership() {
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long forumId = Long.parseLong(csvRow[2]);

            long personId = Long.parseLong(csvRow[3]);

            String creationDateString = csvRow[4];
            Date creationDate = new Date(Long.parseLong(creationDateString));

            Operation<?> operation = new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }

    public static class EventDecoderAddPost implements EventDecoder<Operation<?>> {
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");

        public EventDecoderAddPost() {
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long postId = Long.parseLong(csvRow[2]);

            String imageFile = csvRow[3];

            String creationDateString = csvRow[4];
            Date creationDate = new Date(Long.parseLong(creationDateString));

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

    public static class EventDecoderAddComment implements EventDecoder<Operation<?>> {
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");

        public EventDecoderAddComment() {
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long commentId = Long.parseLong(csvRow[2]);

            String creationDateString = csvRow[3];
            Date creationDate = new Date(Long.parseLong(creationDateString));

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

    public static class EventDecoderAddFriendship implements EventDecoder<Operation<?>> {
        public EventDecoderAddFriendship() {
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));

            long person1Id = Long.parseLong(csvRow[2]);

            long person2Id = Long.parseLong(csvRow[3]);

            String creationDateString = csvRow[4];
            Date creationDate = new Date(Long.parseLong(creationDateString));

            Operation<?> operation = new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    }
}

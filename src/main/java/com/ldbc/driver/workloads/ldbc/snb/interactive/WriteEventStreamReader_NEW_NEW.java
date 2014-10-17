package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader_NEW_NEW;
import com.ldbc.driver.generator.CsvEventStreamReader_NEW_NEW.EventDecoder;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.csv.CharSeeker;
import com.ldbc.driver.util.csv.Extractors;
import com.ldbc.driver.util.csv.Mark;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class WriteEventStreamReader_NEW_NEW implements Iterator<Operation<?>> {
    public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    public static final String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private final CsvEventStreamReader_NEW_NEW<Operation<?>> csvEventStreamReader;

    public WriteEventStreamReader_NEW_NEW(CharSeeker charSeeker, int[] delimiters) {
        Map<String, EventDecoder<Operation<?>>> decoders = new HashMap<>();
        {
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addPersonDecoder = new EventDecoderAddPerson(dateFormat, dateTimeFormat);
            decoders.put("ADD_PERSON", addPersonDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addLikePostDecoder = new EventDecoderAddLikePost(dateTimeFormat);
            decoders.put("ADD_LIKE_POST", addLikePostDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addLikeCommentDecoder = new EventDecoderAddLikeComment(dateTimeFormat);
            decoders.put("ADD_LIKE_COMMENT", addLikeCommentDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addForumDecoder = new EventDecoderAddForum(dateTimeFormat);
            decoders.put("ADD_FORUM", addForumDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addForumMembershipDecoder = new EventDecoderAddForumMembership(dateTimeFormat);
            decoders.put("ADD_FORUM_MEMBERSHIP", addForumMembershipDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addPostDecoder = new EventDecoderAddPost(dateTimeFormat);
            decoders.put("ADD_POST", addPostDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addCommentDecoder = new EventDecoderAddComment(dateTimeFormat);
            decoders.put("ADD_COMMENT", addCommentDecoder);
        }
        {
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
            dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            EventDecoder<Operation<?>> addFriendshipDecoder = new EventDecoderAddFriendship(dateTimeFormat);
            decoders.put("ADD_FRIENDSHIP", addFriendshipDecoder);
        }
        this.csvEventStreamReader = new CsvEventStreamReader_NEW_NEW<>(charSeeker, decoders, delimiters);
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
        private final SimpleDateFormat dateFormat;
        private final SimpleDateFormat dateTimeFormat;
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");
        private final Pattern tupleSeparatorPattern = Pattern.compile(",");
        private final Mark mark;

        public EventDecoderAddPerson(SimpleDateFormat dateFormat, SimpleDateFormat dateTimeFormat) {
            this.dateFormat = dateFormat;
            this.dateTimeFormat = dateTimeFormat;
            this.mark = new Mark();
        }

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long personId;
                if (charSeeker.seek(mark, delimiters)) {
                    personId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                String firstName;
                if (charSeeker.seek(mark, delimiters)) {
                    firstName = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving first name");
                }

                String lastName;
                if (charSeeker.seek(mark, delimiters)) {
                    lastName = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving last name");
                }

                String gender;
                if (charSeeker.seek(mark, delimiters)) {
                    gender = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving gender");
                }

                String birthdayString;
                if (charSeeker.seek(mark, delimiters)) {
                    birthdayString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving birthday");
                }
                Date birthday;
                try {
                    birthday = dateFormat.parse(birthdayString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing birthday string\n%s", birthdayString), e);
                }

                String creationDateString;
                if (charSeeker.seek(mark, delimiters)) {
                    creationDateString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate;
                try {
                    creationDate = dateTimeFormat.parse(creationDateString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing creation date string\n%s", birthdayString), e);
                }

                String locationIp;
                if (charSeeker.seek(mark, delimiters)) {
                    locationIp = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, delimiters)) {
                    browserUsed = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving browser");
                }

                long cityId;
                if (charSeeker.seek(mark, delimiters)) {
                    cityId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving city id");
                }

                String languagesAsString;
                if (charSeeker.seek(mark, delimiters)) {
                    languagesAsString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving language");
                }
                List<String> languages = Lists.newArrayList(collectionSeparatorPattern.split(languagesAsString, -1));

                String emailsAsString;
                if (charSeeker.seek(mark, delimiters)) {
                    emailsAsString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving emails");
                }
                List<String> emails = Lists.newArrayList(collectionSeparatorPattern.split(emailsAsString, -1));

                String tagIdsAsString;
                if (charSeeker.seek(mark, delimiters)) {
                    tagIdsAsString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving tags");
                }
                String[] tagIdsAsStrings = collectionSeparatorPattern.split(tagIdsAsString, -1);
                List<Long> tagIds = new ArrayList<>();
                for (String tagId : tagIdsAsStrings) {
                    tagIds.add(Long.parseLong(tagId));
                }

                String studyAtsAsString;
                if (charSeeker.seek(mark, delimiters)) {
                    studyAtsAsString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving universities");
                }
                String[] studyAtsAsStrings = collectionSeparatorPattern.split(studyAtsAsString, -1);
                List<LdbcUpdate1AddPerson.Organization> studyAts = new ArrayList<>();
                for (String studyAtAsString : studyAtsAsStrings) {
                    String[] studyAtAsStringArray = tupleSeparatorPattern.split(studyAtAsString, -1);
                    studyAts.add(new LdbcUpdate1AddPerson.Organization(
                                    Long.parseLong(studyAtAsStringArray[0]),
                                    Integer.parseInt(studyAtAsStringArray[1])
                            )
                    );
                }

                String workAtsAsString;
                if (charSeeker.seek(mark, delimiters)) {
                    workAtsAsString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving companies");
                }
                String[] workAtsAsStrings = collectionSeparatorPattern.split(workAtsAsString, -1);
                List<LdbcUpdate1AddPerson.Organization> workAts = new ArrayList<>();
                for (String workAtAsString : workAtsAsStrings) {
                    String[] workAtAsStringArray = tupleSeparatorPattern.split(workAtAsString, -1);
                    workAts.add(new LdbcUpdate1AddPerson.Organization(
                                    Long.parseLong(workAtAsStringArray[0]),
                                    Integer.parseInt(workAtAsStringArray[1])
                            )
                    );
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
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add person event", e);
            }
        }
    }

    public static class EventDecoderAddLikePost implements EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Mark mark;

        public EventDecoderAddLikePost(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
            this.mark = new Mark();
        }

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long personId;
                if (charSeeker.seek(mark, delimiters)) {
                    personId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                long postId;
                if (charSeeker.seek(mark, delimiters)) {
                    postId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving post id");
                }

                String creationDateString;
                if (charSeeker.seek(mark, delimiters)) {
                    creationDateString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate;
                try {
                    creationDate = dateTimeFormat.parse(creationDateString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
                }

                Operation<?> operation = new LdbcUpdate2AddPostLike(personId, postId, creationDate);
                operation.setScheduledStartTime(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add post like event", e);
            }
        }
    }

    public static class EventDecoderAddLikeComment implements EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Mark mark;

        public EventDecoderAddLikeComment(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
            this.mark = new Mark();
        }

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long personId;
                if (charSeeker.seek(mark, delimiters)) {
                    personId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                long commentId;
                if (charSeeker.seek(mark, delimiters)) {
                    commentId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving comment id");
                }

                String creationDateString;
                if (charSeeker.seek(mark, delimiters)) {
                    creationDateString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate;
                try {
                    creationDate = dateTimeFormat.parse(creationDateString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
                }

                Operation<?> operation = new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
                operation.setScheduledStartTime(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add comment like event", e);
            }
        }
    }

    public static class EventDecoderAddForum implements EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");
        private final Mark mark;

        public EventDecoderAddForum(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
            this.mark = new Mark();
        }

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long forumId;
                if (charSeeker.seek(mark, delimiters)) {
                    forumId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving forum id");
                }

                String forumTitle;
                if (charSeeker.seek(mark, delimiters)) {
                    forumTitle = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving forum title");
                }

                String creationDateString;
                if (charSeeker.seek(mark, delimiters)) {
                    creationDateString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate;
                try {
                    creationDate = dateTimeFormat.parse(creationDateString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
                }

                long moderatorPersonId;
                if (charSeeker.seek(mark, delimiters)) {
                    moderatorPersonId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving moderator person id");
                }

                String tagIdsAsString;
                if (charSeeker.seek(mark, delimiters)) {
                    tagIdsAsString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving tags");
                }
                String[] tagIdsAsStrings = collectionSeparatorPattern.split(tagIdsAsString, -1);
                List<Long> tagIds = new ArrayList<>();
                for (String tagId : tagIdsAsStrings) {
                    tagIds.add(Long.parseLong(tagId));
                }

                Operation<?> operation = new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIds);
                operation.setScheduledStartTime(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add forum event", e);
            }
        }
    }

    public static class EventDecoderAddForumMembership implements EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Mark mark;

        public EventDecoderAddForumMembership(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
            this.mark = new Mark();
        }

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long forumId;
                if (charSeeker.seek(mark, delimiters)) {
                    forumId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving forum id");
                }

                long personId;
                if (charSeeker.seek(mark, delimiters)) {
                    personId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                String creationDateString;
                if (charSeeker.seek(mark, delimiters)) {
                    creationDateString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate;
                try {
                    creationDate = dateTimeFormat.parse(creationDateString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
                }

                Operation<?> operation = new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
                operation.setScheduledStartTime(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add forum membership event", e);
            }
        }
    }

    public static class EventDecoderAddPost implements EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");
        private final Mark mark;

        public EventDecoderAddPost(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
            this.mark = new Mark();
        }

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long postId;
                if (charSeeker.seek(mark, delimiters)) {
                    postId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving post id");
                }

                String imageFile;
                if (charSeeker.seek(mark, delimiters)) {
                    imageFile = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving image file");
                }

                String creationDateString;
                if (charSeeker.seek(mark, delimiters)) {
                    creationDateString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate;
                try {
                    creationDate = dateTimeFormat.parse(creationDateString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
                }

                String locationIp;
                if (charSeeker.seek(mark, delimiters)) {
                    locationIp = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, delimiters)) {
                    browserUsed = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving browser");
                }

                String language;
                if (charSeeker.seek(mark, delimiters)) {
                    language = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving language");
                }

                String content;
                if (charSeeker.seek(mark, delimiters)) {
                    content = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving content");
                }

                int length;
                if (charSeeker.seek(mark, delimiters)) {
                    length = charSeeker.extract(mark, Extractors.INT);
                } else {
                    throw new GeneratorException("Error retrieving length");
                }

                long authorPersonId;
                if (charSeeker.seek(mark, delimiters)) {
                    authorPersonId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving author person id");
                }

                long forumId;
                if (charSeeker.seek(mark, delimiters)) {
                    forumId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving forum id");
                }

                long countryId;
                if (charSeeker.seek(mark, delimiters)) {
                    countryId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving country id");
                }

                List<Long> tagIds = new ArrayList<>();
                if (charSeeker.seek(mark, delimiters)) {
                    String tagIdsAsString = charSeeker.extract(mark, Extractors.STRING);
                    String[] tagIdsAsStrings = collectionSeparatorPattern.split(tagIdsAsString, -1);
                    for (String tagId : tagIdsAsStrings) {
                        tagIds.add(Long.parseLong(tagId));
                    }
                } else {
                    tagIds = new ArrayList<>();
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
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add post event", e);
            }
        }
    }

    public static class EventDecoderAddComment implements EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Pattern collectionSeparatorPattern = Pattern.compile(";");
        private final Mark mark;

        public EventDecoderAddComment(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
            this.mark = new Mark();
        }

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long commentId;
                if (charSeeker.seek(mark, delimiters)) {
                    commentId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving comment id");
                }

                String creationDateString;
                if (charSeeker.seek(mark, delimiters)) {
                    creationDateString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate;
                try {
                    creationDate = dateTimeFormat.parse(creationDateString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
                }

                String locationIp;
                if (charSeeker.seek(mark, delimiters)) {
                    locationIp = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, delimiters)) {
                    browserUsed = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving browser");
                }

                String content;
                if (charSeeker.seek(mark, delimiters)) {
                    content = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving content");
                }

                int length;
                if (charSeeker.seek(mark, delimiters)) {
                    length = charSeeker.extract(mark, Extractors.INT);
                } else {
                    throw new GeneratorException("Error retrieving length");
                }

                long authorPersonId;
                if (charSeeker.seek(mark, delimiters)) {
                    authorPersonId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving author person id");
                }

                long countryId;
                if (charSeeker.seek(mark, delimiters)) {
                    countryId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving country id");
                }

                long replyOfPostId;
                if (charSeeker.seek(mark, delimiters)) {
                    replyOfPostId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving reply of post id");
                }

                long replyOfCommentId;
                if (charSeeker.seek(mark, delimiters)) {
                    replyOfCommentId = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving reply of comment id");
                }

                List<Long> tagIds = new ArrayList<>();
                if (charSeeker.seek(mark, delimiters)) {
                    String tagIdsAsString = charSeeker.extract(mark, Extractors.STRING);
                    String[] tagIdsAsStrings = collectionSeparatorPattern.split(tagIdsAsString, -1);
                    for (String tagId : tagIdsAsStrings) {
                        tagIds.add(Long.parseLong(tagId));
                    }
                } else {
                    tagIds = new ArrayList<>();
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
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add comment event", e);
            }
        }
    }

    public static class EventDecoderAddFriendship implements EventDecoder<Operation<?>> {
        private final SimpleDateFormat dateTimeFormat;
        private final Mark mark;

        public EventDecoderAddFriendship(SimpleDateFormat dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
            this.mark = new Mark();
        }

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, int[] delimiters) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long person1Id;
                if (charSeeker.seek(mark, delimiters)) {
                    person1Id = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving person id 1");
                }

                long person2Id;
                if (charSeeker.seek(mark, delimiters)) {
                    person2Id = charSeeker.extract(mark, Extractors.LONG);
                } else {
                    throw new GeneratorException("Error retrieving person id 2");
                }

                String creationDateString;
                if (charSeeker.seek(mark, delimiters)) {
                    creationDateString = charSeeker.extract(mark, Extractors.STRING);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate;
                try {
                    creationDate = dateTimeFormat.parse(creationDateString);
                } catch (ParseException e) {
                    throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
                }

                Operation<?> operation = new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
                operation.setScheduledStartTime(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add friendship event", e);
            }
        }
    }
}

package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReaderTimedTypedCharSeeker;
import com.ldbc.driver.generator.CsvEventStreamReaderTimedTypedCharSeeker.EventDecoder;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.csv.CharSeeker;
import com.ldbc.driver.util.csv.Extractors;
import com.ldbc.driver.util.csv.Mark;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class WriteEventStreamReaderCharSeeker implements Iterator<Operation<?>> {
    private final CsvEventStreamReaderTimedTypedCharSeeker<Operation<?>> csvEventStreamReader;

    public WriteEventStreamReaderCharSeeker(CharSeeker charSeeker, Extractors extractors, int columnDelimiter) {
        Map<Integer, EventDecoder<Operation<?>>> decoders = new HashMap<>();
        {
            EventDecoder<Operation<?>> addPersonDecoder = new EventDecoderAddPerson();
            decoders.put(1, addPersonDecoder);
        }
        {
            EventDecoder<Operation<?>> addLikePostDecoder = new EventDecoderAddLikePost();
            decoders.put(2, addLikePostDecoder);
        }
        {
            EventDecoder<Operation<?>> addLikeCommentDecoder = new EventDecoderAddLikeComment();
            decoders.put(3, addLikeCommentDecoder);
        }
        {
            EventDecoder<Operation<?>> addForumDecoder = new EventDecoderAddForum();
            decoders.put(4, addForumDecoder);
        }
        {
            EventDecoder<Operation<?>> addForumMembershipDecoder = new EventDecoderAddForumMembership();
            decoders.put(5, addForumMembershipDecoder);
        }
        {
            EventDecoder<Operation<?>> addPostDecoder = new EventDecoderAddPost();
            decoders.put(6, addPostDecoder);
        }
        {
            EventDecoder<Operation<?>> addCommentDecoder = new EventDecoderAddComment();
            decoders.put(7, addCommentDecoder);
        }
        {
            EventDecoder<Operation<?>> addFriendshipDecoder = new EventDecoderAddFriendship();
            decoders.put(8, addFriendshipDecoder);
        }
        this.csvEventStreamReader = new CsvEventStreamReaderTimedTypedCharSeeker<>(charSeeker, extractors, decoders, columnDelimiter);
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
        private final Pattern tupleSeparatorPattern = Pattern.compile(",");

        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long personId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    personId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                String firstName;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    firstName = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving first name");
                }

                String lastName;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    lastName = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving last name");
                }

                String gender;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    gender = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving gender");
                }

                Long birthdayAsMilli;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    birthdayAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving birthday");
                }
                Date birthday = new Date(birthdayAsMilli);

                long creationDateAsMilli;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    creationDateAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }
                Date creationDate = new Date(creationDateAsMilli);

                String locationIp;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    locationIp = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    browserUsed = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving browser");
                }

                long cityId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    cityId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving city id");
                }

                List<String> languages;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    languages = Lists.newArrayList(charSeeker.extract(mark, extractors.stringArray()).value());
                } else {
                    throw new GeneratorException("Error retrieving languages");
                }

                List<String> emails;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    emails = Lists.newArrayList(charSeeker.extract(mark, extractors.stringArray()).value());
                } else {
                    throw new GeneratorException("Error retrieving emails");
                }

                List<Long> tagIds = new ArrayList<>();
                if (charSeeker.seek(mark, columnDelimiters)) {
                    long[] tagIdsArray = charSeeker.extract(mark, extractors.longArray()).value();
                    for (long tagId : tagIdsArray) {
                        tagIds.add(tagId);
                    }
                } else {
                    throw new GeneratorException("Error retrieving tags");
                }

                // TODO with extractor
                List<LdbcUpdate1AddPerson.Organization> studyAts;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    studyAts = new ArrayList<>();
                    String[] studyAtsAsStrings = charSeeker.extract(mark, extractors.stringArray()).value();
                    for (String studyAtAsString : studyAtsAsStrings) {
                        String[] studyAtAsStringArray = tupleSeparatorPattern.split(studyAtAsString, -1);
                        studyAts.add(new LdbcUpdate1AddPerson.Organization(
                                        Long.parseLong(studyAtAsStringArray[0]),
                                        Integer.parseInt(studyAtAsStringArray[1])
                                )
                        );
                    }
                } else {
                    throw new GeneratorException("Error retrieving universities");
                }

                // TODO with extractor
                List<LdbcUpdate1AddPerson.Organization> workAts;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    workAts = new ArrayList<>();
                    String[] workAtsAsStrings = charSeeker.extract(mark, extractors.stringArray()).value();
                    for (String workAtAsString : workAtsAsStrings) {
                        String[] workAtAsStringArray = tupleSeparatorPattern.split(workAtAsString, -1);
                        workAts.add(new LdbcUpdate1AddPerson.Organization(
                                        Long.parseLong(workAtAsStringArray[0]),
                                        Integer.parseInt(workAtAsStringArray[1])
                                )
                        );
                    }
                } else {
                    throw new GeneratorException("Error retrieving companies");
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
                operation.setScheduledStartTimeAsMilli(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add person event", e);
            }
        }
    }

    public static class EventDecoderAddLikePost implements EventDecoder<Operation<?>> {
        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long personId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    personId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                long postId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    postId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving post id");
                }

                Date creationDate;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    long creationDateAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                    creationDate = new Date(creationDateAsMilli);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }

                Operation<?> operation = new LdbcUpdate2AddPostLike(personId, postId, creationDate);
                operation.setScheduledStartTimeAsMilli(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add post like event", e);
            }
        }
    }

    public static class EventDecoderAddLikeComment implements EventDecoder<Operation<?>> {
        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long personId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    personId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                long commentId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    commentId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving comment id");
                }

                Date creationDate;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    long creationDateAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                    creationDate = new Date(creationDateAsMilli);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }

                Operation<?> operation = new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
                operation.setScheduledStartTimeAsMilli(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add comment like event", e);
            }
        }
    }

    public static class EventDecoderAddForum implements EventDecoder<Operation<?>> {
        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long forumId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    forumId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving forum id");
                }

                String forumTitle;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    forumTitle = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving forum title");
                }

                Date creationDate;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    long creationDateAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                    creationDate = new Date(creationDateAsMilli);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }

                long moderatorPersonId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    moderatorPersonId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving moderator person id");
                }

                List<Long> tagIds;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    tagIds = new ArrayList<>();
                    long[] tagIdsArray = charSeeker.extract(mark, extractors.longArray()).value();
                    for (long tagId : tagIdsArray) {
                        tagIds.add(tagId);
                    }
                } else {
                    throw new GeneratorException("Error retrieving tags");
                }

                Operation<?> operation = new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIds);
                operation.setScheduledStartTimeAsMilli(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add forum event", e);
            }
        }
    }

    public static class EventDecoderAddForumMembership implements EventDecoder<Operation<?>> {
        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long forumId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    forumId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving forum id");
                }

                long personId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    personId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                Date creationDate;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    long creationDateAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                    creationDate = new Date(creationDateAsMilli);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }

                Operation<?> operation = new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
                operation.setScheduledStartTimeAsMilli(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add forum membership event", e);
            }
        }
    }

    public static class EventDecoderAddPost implements EventDecoder<Operation<?>> {
        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long postId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    postId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving post id");
                }

                String imageFile;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    imageFile = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving image file");
                }

                Date creationDate;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    long creationDateAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                    creationDate = new Date(creationDateAsMilli);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }

                String locationIp;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    locationIp = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    browserUsed = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving browser");
                }

                String language;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    language = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving language");
                }

                String content;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    content = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving content");
                }

                int length;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    length = charSeeker.extract(mark, extractors.int_()).intValue();
                } else {
                    throw new GeneratorException("Error retrieving length");
                }

                long authorPersonId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    authorPersonId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving author person id");
                }

                long forumId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    forumId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving forum id");
                }

                long countryId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    countryId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving country id");
                }

                List<Long> tagIds;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    tagIds = new ArrayList<>();
                    long[] tagIdsArray = charSeeker.extract(mark, extractors.longArray()).value();
                    for (long tagId : tagIdsArray) {
                        tagIds.add(tagId);
                    }
                } else {
                    throw new GeneratorException("Error retrieving tags");
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
                operation.setScheduledStartTimeAsMilli(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add post event", e);
            }
        }
    }

    public static class EventDecoderAddComment implements EventDecoder<Operation<?>> {
        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long commentId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    commentId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving comment id");
                }

                Date creationDate;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    long creationDateAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                    creationDate = new Date(creationDateAsMilli);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }

                String locationIp;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    locationIp = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    browserUsed = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving browser");
                }

                String content;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    content = charSeeker.extract(mark, extractors.string()).value();
                } else {
                    throw new GeneratorException("Error retrieving content");
                }

                int length;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    length = charSeeker.extract(mark, extractors.int_()).intValue();
                } else {
                    throw new GeneratorException("Error retrieving length");
                }

                long authorPersonId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    authorPersonId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving author person id");
                }

                long countryId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    countryId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving country id");
                }

                long replyOfPostId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    replyOfPostId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving reply of post id");
                }

                long replyOfCommentId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    replyOfCommentId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving reply of comment id");
                }

                List<Long> tagIds;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    tagIds = new ArrayList<>();
                    long[] tagIdsArray = charSeeker.extract(mark, extractors.longArray()).value();
                    for (long tagId : tagIdsArray) {
                        tagIds.add(tagId);
                    }
                } else {
                    throw new GeneratorException("Error retrieving tags");
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
                operation.setScheduledStartTimeAsMilli(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add comment event", e);
            }
        }
    }

    public static class EventDecoderAddFriendship implements EventDecoder<Operation<?>> {
        @Override
        public Operation<?> decodeEvent(long scheduledStartTime, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                Time eventDueTime = Time.fromMilli(scheduledStartTime);

                long person1Id;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    person1Id = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving person id 1");
                }

                long person2Id;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    person2Id = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving person id 2");
                }

                Date creationDate;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    long creationDateAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                    creationDate = new Date(creationDateAsMilli);
                } else {
                    throw new GeneratorException("Error retrieving creation date");
                }

                Operation<?> operation = new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
                operation.setScheduledStartTimeAsMilli(eventDueTime);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add friendship event", e);
            }
        }
    }
}

package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderTimedTypedCharSeeker;
import com.ldbc.driver.generator.CsvEventStreamReaderTimedTypedCharSeeker.EventDecoder;
import com.ldbc.driver.generator.GeneratorException;

import java.io.IOException;
import java.util.*;

public class WriteEventStreamReaderCharSeeker {
    public static Iterator<Operation> create(CharSeeker charSeeker, Extractors extractors, int columnDelimiter) {
        Map<Integer, EventDecoder<Operation>> decoders = new HashMap<>();
        decoders.put(1, new EventDecoderAddPerson());
        decoders.put(2, new EventDecoderAddLikePost());
        decoders.put(3, new EventDecoderAddLikeComment());
        decoders.put(4, new EventDecoderAddForum());
        decoders.put(5, new EventDecoderAddForumMembership());
        decoders.put(6, new EventDecoderAddPost());
        decoders.put(7, new EventDecoderAddComment());
        decoders.put(8, new EventDecoderAddFriendship());
        return new CsvEventStreamReaderTimedTypedCharSeeker<>(charSeeker, extractors, decoders, columnDelimiter);
    }

    public static class EventDecoderAddPerson implements EventDecoder<Operation> {

        @Override
        public Operation decodeEvent(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                long personId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    personId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving person id");
                }

                String firstName;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    firstName = charSeeker.extract(mark, extractors.string()).value();
                    if (null == firstName) firstName = "";
                } else {
                    throw new GeneratorException("Error retrieving first name");
                }

                String lastName;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    lastName = charSeeker.extract(mark, extractors.string()).value();
                    if (null == lastName) lastName = "";
                } else {
                    throw new GeneratorException("Error retrieving last name");
                }

                String gender;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    gender = charSeeker.extract(mark, extractors.string()).value();
                    if (null == gender) gender = "";
                } else {
                    throw new GeneratorException("Error retrieving gender");
                }

                Long birthdayAsMilli;
                if (charSeeker.seek(mark, columnDelimiters)) {
		    String birthdayAsMilliStr = charSeeker.extract(mark, extractors.string()).value();
                    if (null == birthdayAsMilliStr) birthdayAsMilli = null;
		    else birthdayAsMilli = Long.parseLong(birthdayAsMilliStr);
                    //birthdayAsMilli = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving birthday");
                }
		Date birthday = null;
		if (birthdayAsMilli != null)
		    birthday = new Date(birthdayAsMilli);

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
                    if (null == locationIp) locationIp = "";
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    browserUsed = charSeeker.extract(mark, extractors.string()).value();
                    if (null == browserUsed) browserUsed = "";
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

                List<LdbcUpdate1AddPerson.Organization> studyAts;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    studyAts = new ArrayList<>();
                    int[][] studyAtsArray = charSeeker.extract(mark, extractors.intTupleArray(2)).value();
                    for (int i = 0; i < studyAtsArray.length; i++) {
                        studyAts.add(new LdbcUpdate1AddPerson.Organization(
                                        studyAtsArray[i][0],
                                        studyAtsArray[i][1]
                                )
                        );
                    }
                } else {
                    throw new GeneratorException("Error retrieving universities");
                }

                List<LdbcUpdate1AddPerson.Organization> workAts;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    workAts = new ArrayList<>();
                    int[][] workAtsArray = charSeeker.extract(mark, extractors.intTupleArray(2)).value();
                    for (int i = 0; i < workAtsArray.length; i++) {
                        workAts.add(new LdbcUpdate1AddPerson.Organization(
                                        workAtsArray[i][0],
                                        workAtsArray[i][1]
                                )
                        );
                    }
                } else {
                    throw new GeneratorException("Error retrieving companies");
                }

                Operation operation = new LdbcUpdate1AddPerson(
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
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add person event", e);
            }
        }
    }

    public static class EventDecoderAddLikePost implements EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
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

                Operation operation = new LdbcUpdate2AddPostLike(personId, postId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add post like event", e);
            }
        }
    }

    public static class EventDecoderAddLikeComment implements EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
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

                Operation operation = new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add comment like event", e);
            }
        }
    }

    public static class EventDecoderAddForum implements EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                long forumId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    forumId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving forum id");
                }

                String forumTitle;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    forumTitle = charSeeker.extract(mark, extractors.string()).value();
                    if (null == forumTitle) forumTitle = "";
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

                Operation operation = new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIds);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add forum event", e);
            }
        }
    }

    public static class EventDecoderAddForumMembership implements EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
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

                Operation operation = new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add forum membership event", e);
            }
        }
    }

    public static class EventDecoderAddPost implements EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
                long postId;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    postId = charSeeker.extract(mark, extractors.long_()).longValue();
                } else {
                    throw new GeneratorException("Error retrieving post id");
                }

                String imageFile;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    imageFile = charSeeker.extract(mark, extractors.string()).value();
                    if (null == imageFile) imageFile = "";
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
                    if (null == locationIp) locationIp = "";
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    browserUsed = charSeeker.extract(mark, extractors.string()).value();
                    if (null == browserUsed) browserUsed = "";
                } else {
                    throw new GeneratorException("Error retrieving browser");
                }

                String language;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    language = charSeeker.extract(mark, extractors.string()).value();
                    if (null == language) language = "";
                } else {
                    throw new GeneratorException("Error retrieving language");
                }

                String content;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    content = charSeeker.extract(mark, extractors.string()).value();
                    if (null == content) content = "";
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
		    String countryIdStr = charSeeker.extract(mark, extractors.string()).value();
                    if (null == countryIdStr) countryId = -1;
		    else countryId = Long.parseLong(countryIdStr);
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

		List<Long> mentionedIds;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    mentionedIds = new ArrayList<>();
                    long[] mentionedIdsArray = charSeeker.extract(mark, extractors.longArray()).value();
                    for (long mentionedId : mentionedIdsArray) {
                        mentionedIds.add(mentionedId);
                    }
                } else {
                    throw new GeneratorException("Error retrieving mentioned");
                }

		Boolean privacy;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    String tmp = charSeeker.extract(mark, extractors.string()).value();
                    if (null == tmp) privacy = null;
		    if ("true" == tmp) privacy = true;
		    else privacy = false;
                } else {
                    throw new GeneratorException("Error retrieving gif");
                }

		String link;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    link = charSeeker.extract(mark, extractors.string()).value();
                    if (null == link) link = "";
                } else {
                    throw new GeneratorException("Error retrieving link");
                }


                Operation operation = new LdbcUpdate6AddPost(
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
                        tagIds,
			mentionedIds,
			privacy,
			link);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add post event", e);
            }
        }
    }

    public static class EventDecoderAddComment implements EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
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
                    if (null == locationIp) locationIp = "";
                } else {
                    throw new GeneratorException("Error retrieving location ip");
                }

                String browserUsed;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    browserUsed = charSeeker.extract(mark, extractors.string()).value();
                    if (null == browserUsed) browserUsed = "";
                } else {
                    throw new GeneratorException("Error retrieving browser");
                }

                String content;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    content = charSeeker.extract(mark, extractors.string()).value();
                    if (null == content) content = "";
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
		    String countryIdStr = charSeeker.extract(mark, extractors.string()).value();
                    if (null == countryIdStr) countryId = -1;
		    else countryId = Long.parseLong(countryIdStr);
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

		List<Long> mentionedIds;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    mentionedIds = new ArrayList<>();
                    long[] mentionedIdsArray = charSeeker.extract(mark, extractors.longArray()).value();
                    for (long mentionedId : mentionedIdsArray) {
                        mentionedIds.add(mentionedId);
                    }
                } else {
                    throw new GeneratorException("Error retrieving mentioned");
                }

		Boolean privacy;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    String tmp = charSeeker.extract(mark, extractors.string()).value();
                    if (null == tmp) privacy = null;
		    if ("true" == tmp) privacy = true;
		    else privacy = false;
                } else {
                    throw new GeneratorException("Error retrieving privacy");
                }

		String link;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    link = charSeeker.extract(mark, extractors.string()).value();
                    if (null == link) link = "";
                } else {
                    throw new GeneratorException("Error retrieving link");
                }

		String gif;
                if (charSeeker.seek(mark, columnDelimiters)) {
                    gif = charSeeker.extract(mark, extractors.string()).value();
                    if (null == gif) gif = "";
                } else {
                    throw new GeneratorException("Error retrieving gif");
                }

		
                Operation operation = new LdbcUpdate7AddComment(
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
                        tagIds,
			mentionedIds,
			privacy,
			link,
			gif);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add comment event", e);
            }
        }
    }

    public static class EventDecoderAddFriendship implements EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) {
            try {
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

                Operation operation = new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (IOException e) {
                throw new GeneratorException("Error parsing add friendship event", e);
            }
        }
    }
}

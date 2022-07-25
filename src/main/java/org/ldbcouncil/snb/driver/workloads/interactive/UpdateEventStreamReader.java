package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * UpdateEventStreamReader.java
 *
 * Decoder for the update events. There are 8 insert events and 8 delete events.
 * Each event has a separate decoder that decodes a java.sql.Resultset
 */

import org.ldbcouncil.snb.driver.generator.EventStreamReader;
import org.ldbcouncil.snb.driver.generator.EventStreamReader.EventDecoder;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import static java.lang.String.format;

public class UpdateEventStreamReader implements Iterator<Operation>
{
    /**
     * Get the first attribute of the ResultSet rs representing an operation's date.
     */
    static long getOperationDate(ResultSet rs) throws SQLException {
        return rs.getLong(1);
    }

    static long getDependencyTimeStamp(ResultSet rs) throws SQLException {
        return rs.getLong(2);
    }

    private final Iterator<Operation> operationStream;

    public UpdateEventStreamReader( Iterator<Operation> operationStream )
    {
        this.operationStream = operationStream;
    }

    @Override
    public boolean hasNext()
    {
        return operationStream.hasNext();
    }

    @Override
    public Operation next()
    {
        return operationStream.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    public static Map<Class<? extends Operation>, EventDecoder<Operation>> getDecoders(){
        Map<Class<? extends Operation>, EventDecoder<Operation>> decoders = new HashMap<>();
        decoders.put(LdbcInsert1AddPerson.class,      new EventDecoderAddPerson());
        decoders.put(LdbcInsert2AddPostLike.class,    new EventDecoderAddLikePost());
        decoders.put(LdbcInsert3AddCommentLike.class, new EventDecoderAddLikeComment());
        decoders.put(LdbcInsert4AddForum.class,       new EventDecoderAddForum());
        decoders.put(LdbcInsert5AddForumMembership.class, new EventDecoderAddForumMembership());
        decoders.put(LdbcInsert6AddPost.class,        new EventDecoderAddPost());
        decoders.put(LdbcInsert7AddComment.class,     new EventDecoderAddComment());
        decoders.put(LdbcInsert8AddFriendship.class,  new EventDecoderAddFriendship());
        decoders.put(LdbcDelete1RemovePerson.class,   new EventDecoderDeletePerson());
        decoders.put(LdbcDelete2RemovePostLike.class, new EventDecoderDeletePostLike());
        decoders.put(LdbcDelete3RemoveCommentLike.class, new EventDecoderDeleteCommentLike());
        decoders.put(LdbcDelete4RemoveForum.class, new EventDecoderDeleteForum());
        decoders.put(LdbcDelete5RemoveForumMembership.class, new EventDecoderDeleteForumMembership());
        decoders.put(LdbcDelete6RemovePostThread.class, new EventDecoderDeletePostThread());
        decoders.put(LdbcDelete7RemoveCommentSubthread.class, new EventDecoderDeleteCommentSubThread());
        decoders.put(LdbcDelete8RemoveFriendship.class, new EventDecoderDeleteFriendship());
        return decoders;
    }

    public static class EventDecoderAddPerson implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long personId = rs.getLong(3);
                String firstName = rs.getString(4);
                String lastName = rs.getString(5);
                String gender = rs.getString(6);
                long birthdayAsMilli = rs.getDate(7).getTime();
                Date birthday = new Date(birthdayAsMilli);
                Date creationDate = new Date(scheduledStartTimeAsMilli);
                String locationIp = rs.getString(8);
                String browserUsed = rs.getString(9);
                long cityId = rs.getLong(10);

                List<String> languages;
                String languagesResult = rs.getString(11);
                if (languagesResult != null && !languagesResult.isEmpty()) {
                    String[] languagesArray = languagesResult.split(";");
                    languages = Arrays.asList(languagesArray);
                }
                else {
                    languages = new ArrayList<>();
                }

                List<String> emails;
                String emailsResult = rs.getString(12);
                if (emailsResult != null && !emailsResult.isEmpty()) {
                    String[] emailsArray = emailsResult.split(";");
                    emails = Arrays.asList(emailsArray);
                }
                else {
                    emails = new ArrayList<>();
                }

                List<Long> tagIds = new ArrayList<>();
                String tagIdsResult = rs.getString(13);
                if (tagIdsResult != null && !tagIdsResult.isEmpty()) {
                    String[] tagIdsArrays = tagIdsResult.split(";");
                    for (String value : tagIdsArrays) {
                        tagIds.add(Long.parseLong(value));
                    }
                }

                List<LdbcInsert1AddPerson.Organization> studyAts = new ArrayList<>();
                String studyAtsResult = rs.getString(14);
                if (studyAtsResult != null && !studyAtsResult.isEmpty()) {
                    String[] studyAtsArray = studyAtsResult.split(";");
                    List<String> studyAtsStrings = Arrays.asList(studyAtsArray);
                    for (String study : studyAtsStrings) {
                        String[] studySplit = study.split(",");
                        studyAts.add(new LdbcInsert1AddPerson.Organization(Long.parseLong(studySplit[0]), Integer.parseInt(studySplit[1])));
                    }
                }

                List<LdbcInsert1AddPerson.Organization> workAts = new ArrayList<>();
                String workAtsResult = rs.getString(15);
                if (workAtsResult != null && !workAtsResult.isEmpty()) {
                    String[] workAtsArray =  workAtsResult.split(";");
                    List<String> workAtsStrings = Arrays.asList(workAtsArray);
                    for (String work : workAtsStrings) {
                        String[] workSplit = work.split(",");
                        workAts.add(new LdbcInsert1AddPerson.Organization(Long.parseLong(workSplit[0]), Integer.parseInt(workSplit[1])));
                    }
                }

                Operation operation = new LdbcInsert1AddPerson(
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
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcInsert1AddPerson: %s", e));
            }
        }
    }

    public static class EventDecoderAddLikePost implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long personId = rs.getLong(3);
                long postId = rs.getLong(4);
                Date creationDate = new Date(scheduledStartTimeAsMilli); // Same as scheduled time

                Operation operation = new LdbcInsert2AddPostLike(personId, postId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcInsert2AddPostLike: %s", e));
            }
        }
    }

    public static class EventDecoderAddLikeComment implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long personId = rs.getLong(3);
                long commentId = rs.getLong(4);
                Date creationDate = new Date(scheduledStartTimeAsMilli);

                Operation operation = new LdbcInsert3AddCommentLike(personId, commentId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcInsert3AddCommentLike: %s", e));
            }
        }
    }

    public static class EventDecoderAddForum implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long forumId = rs.getLong(3);
                String forumTitle = rs.getString(4);
                Date creationDate = new Date(scheduledStartTimeAsMilli); // Same as scheduled time
                long moderatorPersonId = rs.getLong(5);

                List<Long> tagIds = new ArrayList<>();
                String tagIdsResult = rs.getString(6);
                if (tagIdsResult != null && !tagIdsResult.isEmpty()) {
                    String[] tagIdsArrays = tagIdsResult.split(";");
                    for (String value : tagIdsArrays) {
                        tagIds.add(Long.parseLong(value));
                    }
                }

                Operation operation = new LdbcInsert4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIds);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcInsert4AddForum: %s", e));
            }
        }
    }

    public static class EventDecoderAddForumMembership  implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long personId = rs.getLong(3);
                long forumId = rs.getLong(4);
                Date creationDate = new Date(scheduledStartTimeAsMilli);

                Operation operation = new LdbcInsert5AddForumMembership(forumId, personId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcInsert5AddForumMembership: %s", e));
            }
        }
    }

    public static class EventDecoderAddPost implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long postId = rs.getLong(3);
                String imageFile = rs.getString(4);
                Date creationDate = new Date(scheduledStartTimeAsMilli); // Same as scheduled
                String locationIp = rs.getString(5);
                String browserUsed = rs.getString(6);
                String language = rs.getString(7);
                String content = rs.getString(8);
                int length = rs.getInt(9);
                long authorPersonId = rs.getLong(10);
                long forumId = rs.getLong(11);
                long countryId = rs.getLong(12);

                List<Long> tagIds = new ArrayList<>();
                String tagIdsResult = rs.getString(13);
                if (tagIdsResult != null && !tagIdsResult.isEmpty()) {
                    String[] tagIdsArrays = tagIdsResult.split(";");
                    for (String value : tagIdsArrays) {
                        tagIds.add(Long.parseLong(value));
                    }
                }

                Operation operation = new LdbcInsert6AddPost(
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
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcInsert6AddPost: %s", e));
            }
        }
    }

    public static class EventDecoderAddComment implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long commentId = rs.getLong(3);
                Date creationDate = new Date(scheduledStartTimeAsMilli); // Same as scheduled time
                String locationIp = rs.getString(4);
                String browserUsed = rs.getString(5);
                String content = rs.getString(6);
                int length = rs.getInt(7);
                long authorPersonId = rs.getLong(8);
                long countryId = rs.getLong(9);
                long replyOfPostId = rs.getLong(10);
                long replyOfCommentId = rs.getLong(11);

                List<Long> tagIds = new ArrayList<>();
                String tagIdsResult = rs.getString(12);
                if (tagIdsResult != null && !tagIdsResult.isEmpty()) {
                    String[] tagIdsArrays = tagIdsResult.split(";");
                    for (String value : tagIdsArrays) {
                        tagIds.add(Long.parseLong(value));
                    }
                }

                Operation operation = new LdbcInsert7AddComment(
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
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcInsert7AddComment: %s", e));
            }
        }
    }

    public static class EventDecoderAddFriendship implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long person1Id = rs.getLong(3);
                long person2Id = rs.getLong(4);
                Date creationDate = new Date(scheduledStartTimeAsMilli);

                Operation operation = new LdbcInsert8AddFriendship(person1Id, person2Id, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcInsert8AddFriendship: %s", e));
            }
        }
    }

    // Delete operation decoders
    public static class EventDecoderDeletePerson implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long personId = rs.getLong(3);

                Operation operation = new LdbcDelete1RemovePerson(personId);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcDelete1RemovePerson: %s", e));
            }
        }
    }

    public static class EventDecoderDeletePostLike implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long personId = rs.getLong(3);
                long postId = rs.getLong(4);

                Operation operation = new LdbcDelete2RemovePostLike(personId, postId);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcDelete2RemovePostLike: %s", e));
            }
        }
    }

    public static class EventDecoderDeleteCommentLike implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long personId = rs.getLong(3);
                long commentId = rs.getLong(4);

                Operation operation = new LdbcDelete3RemoveCommentLike(personId, commentId);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcDelete3RemoveCommentLike: %s", e));
            }
        }
    }

    public static class EventDecoderDeleteForum implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long forumId = rs.getLong(3);

                Operation operation = new LdbcDelete4RemoveForum(forumId);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcDelete4RemoveForum: %s", e));
            }
        }
    }

    public static class EventDecoderDeleteForumMembership implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long forumId = rs.getLong(3);
                long personId = rs.getLong(4);

                Operation operation = new LdbcDelete5RemoveForumMembership(forumId, personId);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcDelete5RemoveForumMembership: %s", e));
            }
        }
    }

    public static class EventDecoderDeletePostThread implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long postId = rs.getLong(3);

                Operation operation = new LdbcDelete6RemovePostThread(postId);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcDelete6RemovePostThread: %s", e));
            }
        }
    }

    public static class EventDecoderDeleteCommentSubThread implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long commentId = rs.getLong(3);

                Operation operation = new LdbcDelete7RemoveCommentSubthread(commentId);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcDelete7RemoveCommentSubthread: %s", e));
            }
        }
    }

    public static class EventDecoderDeleteFriendship implements EventStreamReader.EventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException
        {
            try
            {
                long scheduledStartTimeAsMilli = getOperationDate(rs);
                long dependencyTimeStamp = getDependencyTimeStamp(rs);
                long person1Id = rs.getLong(3);
                long person2Id = rs.getLong(4);

                Operation operation = new LdbcDelete8RemoveFriendship(person1Id, person2Id);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeStamp); 
                return operation;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for LdbcDelete8RemoveFriendship: %s", e));
            }
        }
    }
}

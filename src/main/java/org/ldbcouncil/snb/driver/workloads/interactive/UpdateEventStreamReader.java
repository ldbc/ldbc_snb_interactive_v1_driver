package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * UpdateEventStreamReader.java
 * 
 * Entry decoder for the update events. There are 8 update events.
 * In this class, the decoder first decodes the event type column in the result-row
 * to determine the type of operation. It then passes the data to the 
 * appropiate decoder.
 */
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;

import com.google.common.collect.Ordering;

import org.ldbcouncil.snb.driver.generator.GeneratorException;
import org.ldbcouncil.snb.driver.generator.UpdateEventStreamDecoder;
import org.ldbcouncil.snb.driver.generator.UpdateEventStreamDecoder.UpdateEventDecoder;

import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcUpdate1AddPerson;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcUpdate2AddPostLike;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcUpdate3AddCommentLike;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcUpdate4AddForum;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcUpdate5AddForumMembership;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcUpdate6AddPost;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcUpdate7AddComment;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcUpdate8AddFriendship;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.lang.String.format;

public class UpdateEventStreamReader implements Iterator<Operation>
{
    private final Iterator<Operation> objectArray;

    public UpdateEventStreamReader( Iterator<Operation> objectArray )
    {
        this.objectArray = objectArray;
    }

    @Override
    public boolean hasNext()
    {
        return objectArray.hasNext();
    }

    @Override
    public Operation next()
    {
        return objectArray.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    /**
     * Inner class used for decoding Resultset data for query 1 parameters.
     */
    public static class EventDecoder implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation>
    {
        private final UpdateEventDecoder<Operation>[] decoders;

        // personId|firstName
        // 2199032251700|Andrea
        public EventDecoder(Map<Integer,UpdateEventDecoder<Operation>> decoders)
        {
            int minEventTypeCode = Ordering.<Integer>natural().min( decoders.keySet() );
            int maxEventTypeCode = Ordering.<Integer>natural().max( decoders.keySet() );
            if ( minEventTypeCode < 0 )
            {
                throw new GeneratorException( "Event codes must be positive numbers: " + decoders.keySet().toString() );
            }
            this.decoders = new UpdateEventDecoder[maxEventTypeCode + 1];
            for ( Integer eventTypeCode : decoders.keySet() )
            {
                this.decoders[eventTypeCode] = decoders.get( eventTypeCode );
            }
        }

        /**
         * @param rs: Resultset object containing the row to decode
        * @return Object array
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                int eventType = rs.getInt(3);

                UpdateEventDecoder<Operation> decoder = decoders[eventType];
                if ( null == decoder )
                {
                    throw new NoSuchElementException(
                            format( "No decoder found that matches this column\nDECODER KEY: %s", eventType )
                    );
                }
                return decoder.decodeEvent(rs);

            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query1Event: %s", e));
            }
        }
    }

    public static class EventDecoderAddPerson implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation> {
        //1347584779167|0|1|35184372097711|Wilhelm|Fischer|male|350611200000|1347584779167|46.21.6.39|Firefox|621|de;en|
        //Wilhelm35184372097711@gmail.com;Wilhelm35184372097711@gmx.com;Wilhelm35184372097711@hotmail.com;Wilhelm35184372097711@yahoo.com|1419||450,2008

        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException {
            try {
                long scheduledStartTimeAsMilli = rs.getLong(1);
                long dependencyTimeAsMilli = rs.getLong(2);
                // Skip index 3, evenType
                long personId = rs.getLong(4);
                String firstName = rs.getString(5);
                String lastName = rs.getString(6);
                String gender = rs.getString(7);
                long birthdayAsMilli = rs.getLong(8);
                Date birthday = new Date(birthdayAsMilli);
                long creationDateAsMilli = rs.getLong(9);
                Date creationDate = new Date(creationDateAsMilli);
                String locationIp = rs.getString(10);
                String browserUsed = rs.getString(11);
                long cityId = rs.getLong(12);
                
                // Arrays
                Array languagesResult = rs.getArray(13); 
                String[] languagesArray = (String[]) languagesResult.getArray();
                List<String> languages = Arrays.asList(languagesArray);

                Array emailsResult = rs.getArray(14); 
                String[] emailsArray = (String[]) emailsResult.getArray();
                List<String> emails = Arrays.asList(emailsArray);

                Array tagIdsResult = rs.getArray(15); 
                Long[] tagIdsArrays = (Long[]) tagIdsResult.getArray();
                List<Long> tagIds = Arrays.asList(tagIdsArrays);

                Array studyAtsResult = rs.getArray(16); 
                String[] studyAtsArray = (String[]) studyAtsResult.getArray();
                List<String> studyAtsStrings = Arrays.asList(studyAtsArray);
                List<LdbcUpdate1AddPerson.Organization> studyAts = new ArrayList<>();
                for (String study : studyAtsStrings) {
                    String[] studySplit = study.split(",");
                    studyAts.add(new LdbcUpdate1AddPerson.Organization(Long.parseLong(studySplit[0]), Integer.parseInt(studySplit[1])));
                }

                Array workAtsResult = rs.getArray(17); 
                String[] workAtsArray = (String[]) workAtsResult.getArray();
                List<String> workAtsStrings = Arrays.asList(workAtsArray);
                List<LdbcUpdate1AddPerson.Organization> workAts = new ArrayList<>();
                for (String work : workAtsStrings) {
                    String[] workSplit = work.split(",");
                    workAts.add(new LdbcUpdate1AddPerson.Organization(Long.parseLong(workSplit[0]), Integer.parseInt(workSplit[1])));
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
            } catch (SQLException e) {
                throw new WorkloadException("Error parsing add person event", e);
            }
        }
    }

    public static class EventDecoderAddLikePost implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation> {
        // 1347528982194|1329938741248|2|26388279073665|1236953235741|1347528982194
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException {
            try {
                long scheduledStartTimeAsMilli = rs.getLong(1);
                long dependencyTimeAsMilli = rs.getLong(2);
                // Skip index 3, evenType
                long personId = rs.getLong(4);
                long postId = rs.getLong(5);
                long creationDateAsMilli = rs.getLong(6);
                Date creationDate = new Date(creationDateAsMilli);

                Operation operation = new LdbcUpdate2AddPostLike(personId, postId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (SQLException e) {
                throw new WorkloadException("Error parsing add post like event", e);
            }
        }
    }

    public static class EventDecoderAddLikeComment implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation> {
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException{
            try {
                long scheduledStartTimeAsMilli = rs.getLong(1);
                long dependencyTimeAsMilli = rs.getLong(2);
                // Skip index 3, evenType
                long personId = rs.getLong(4);
                long commentId = rs.getLong(5);
                long creationDateAsMilli = rs.getLong(6);
                Date creationDate = new Date(creationDateAsMilli);

                Operation operation = new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (SQLException e) {
                throw new WorkloadException("Error parsing add comment like event", e);
            }
        }
    }

    public static class EventDecoderAddForum implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation> {
        // 1356836924126|1291176246893|4|2336462231818|Album 12 of K. Kumar|1356836924126|10995116281872|568
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException{
            try {
                long scheduledStartTimeAsMilli = rs.getLong(1);
                long dependencyTimeAsMilli = rs.getLong(2);
                // Skip index 3, evenType
                long forumId = rs.getLong(4);
                String forumTitle = rs.getString(5);
                long creationDateAsMilli = rs.getLong(6);
                Date creationDate = new Date(creationDateAsMilli);
                long moderatorPersonId = rs.getLong(7);

                Array tagIdsResult = rs.getArray(8); 
                Long[] tagIdsArrays = (Long[]) tagIdsResult.getArray();
                List<Long> tagIds = Arrays.asList(tagIdsArrays);

                Operation operation = new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIds);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (SQLException e) {
                throw new WorkloadException("Error parsing add forum event", e);
            }
        }
    }

    public static class EventDecoderAddForumMembership  implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation> {
        // 1347953761820|1329795612316|5|962072684758|26388279077503|1347953761820
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException{
            try {
                long scheduledStartTimeAsMilli = rs.getLong(1);
                long dependencyTimeAsMilli = rs.getLong(2);
                // Skip index 3, evenType
                long forumId = rs.getLong(4);
                long personId = rs.getLong(5);
                long creationDateAsMilli = rs.getLong(6);
                Date creationDate = new Date(creationDateAsMilli);

                Operation operation = new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (SQLException e) {
                throw new WorkloadException("Error parsing add forum membership event", e);
            }
        }
    }

    public static class EventDecoderAddPost implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation> {
        // 1355597870966|1350647897496|6|2336464885268||1355597870966|31.192.131.147|Internet Explorer|uz
        // |About James Cook, February 1779) was a British explorer, navigator and cartographer who ultimately rose to |107|35184372098258|1511828536084|56|2012
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException{
            try {
                long scheduledStartTimeAsMilli = rs.getLong(1);
                long dependencyTimeAsMilli = rs.getLong(2);
                // Skip index 3, evenType
                long postId = rs.getLong(4);
                String imageFile = rs.getString(5);
                long creationDateAsMilli = rs.getLong(6);
                Date creationDate = new Date(creationDateAsMilli);
                String locationIp = rs.getString(7);
                String browserUsed = rs.getString(8);
                String language = rs.getString(9);
                String content = rs.getString(10);
                int length = rs.getInt(11);
                long authorPersonId = rs.getLong(12);
                long forumId = rs.getLong(13);
                long countryId = rs.getLong(14);
                
                Array tagIdsResult = rs.getArray(15); 
                Long[] tagIdsArrays = (Long[]) tagIdsResult.getArray();
                List<Long> tagIds = Arrays.asList(tagIdsArrays);

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
                        tagIds);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (SQLException e) {
                throw new WorkloadException("Error parsing add post event", e);
            }
        }
    }

    public static class EventDecoderAddComment implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation> {
        // 1356837312993|1349744035667|7|2336463358542|1356837312993|164.41.63.169|Chrome|good|4|35184372093856|49|2336463358541|-1|
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException{
            try {
                long scheduledStartTimeAsMilli = rs.getLong(1);
                long dependencyTimeAsMilli = rs.getLong(2);
                // Skip index 3, evenType
                long commentId = rs.getLong(4);
                long creationDateAsMilli = rs.getLong(5);
                Date creationDate = new Date(creationDateAsMilli);
                String locationIp = rs.getString(6);
                String browserUsed = rs.getString(7);
                String content = rs.getString(8);
                int length = rs.getInt(9);
                long authorPersonId = rs.getLong(10);
                long countryId = rs.getLong(11);
                long replyOfPostId = rs.getLong(12);
                long replyOfCommentId = rs.getLong(13);
                
                Array tagIdsResult = rs.getArray(14); 
                Long[] tagIdsArrays = (Long[]) tagIdsResult.getArray();
                List<Long> tagIds = Arrays.asList(tagIdsArrays);

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
                        tagIds);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (SQLException e) {
                throw new WorkloadException("Error parsing add comment event", e);
            }
        }
    }

    public static class EventDecoderAddFriendship implements UpdateEventStreamDecoder.UpdateEventDecoder<Operation> {
        // 1353961176873|1353391211070|8|8796093024336|37383395351049|1353961176873
        @Override
        public Operation decodeEvent(ResultSet rs) throws WorkloadException{
            try {
                long scheduledStartTimeAsMilli = rs.getLong(1);
                long dependencyTimeAsMilli = rs.getLong(2);
                // Skip index 3, evenType
                long person1Id = rs.getLong(4);
                long person2Id = rs.getLong(5);
                long creationDateAsMilli = rs.getLong(6);
                Date creationDate = new Date(creationDateAsMilli);

                Operation operation = new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
                operation.setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
                operation.setTimeStamp(scheduledStartTimeAsMilli);
                operation.setDependencyTimeStamp(dependencyTimeAsMilli);
                return operation;
            } catch (SQLException e) {
                throw new WorkloadException("Error parsing add friendship event", e);
            }
        }
    }
}

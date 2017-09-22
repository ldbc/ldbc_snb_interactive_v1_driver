package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReaderTimedTypedCsvReader;
import com.ldbc.driver.generator.CsvEventStreamReaderTimedTypedCsvReader.EventDecoder;
import com.ldbc.driver.util.Function1;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class WriteEventStreamReaderRegex
{
    private static final List<String> EMPTY_LIST = new ArrayList<>();

    public static Iterator<Operation> create( Iterator<String[]> csvRowIterator )
    {
        Map<String,EventDecoder<Operation>> decoders = new HashMap<>();
        decoders.put( "1", new EventDecoderAddPerson() );
        decoders.put( "2", new EventDecoderAddLikePost() );
        decoders.put( "3", new EventDecoderAddLikeComment() );
        decoders.put( "4", new EventDecoderAddForum() );
        decoders.put( "5", new EventDecoderAddForumMembership() );
        decoders.put( "6", new EventDecoderAddPost() );
        decoders.put( "7", new EventDecoderAddComment() );
        decoders.put( "8", new EventDecoderAddFriendship() );
        Function1<String[],String,RuntimeException> decoderKeyExtractor =
                new Function1<String[],String,RuntimeException>()
                {
                    @Override
                    public String apply( String[] csvRow )
                    {
                        return csvRow[2];
                    }
                };
        return new CsvEventStreamReaderTimedTypedCsvReader<>( csvRowIterator, decoders, decoderKeyExtractor );
    }

    public static class EventDecoderAddPerson implements EventDecoder<Operation>
    {
        private final Pattern collectionSeparatorPattern = Pattern.compile( ";" );
        private final Pattern tupleSeparatorPattern = Pattern.compile( "," );

        public EventDecoderAddPerson()
        {
        }

        @Override
        public Operation decodeEvent( String[] csvRow )
        {
            long scheduledStartTimeAsMilli = Long.parseLong( csvRow[0] );
            long dependencyTimeAsMilli = Long.parseLong( csvRow[1] );

            long personId = Long.parseLong( csvRow[3] );

            String firstName = csvRow[4];

            String lastName = csvRow[5];

            String gender = csvRow[6];

            String birthdayString = csvRow[7];
            Date birthday = new Date( Long.parseLong( birthdayString ) );

            String creationDateString = csvRow[8];
            Date creationDate = new Date( Long.parseLong( creationDateString ) );

            String locationIp = csvRow[9];

            String browserUsed = csvRow[10];

            long cityId = Long.parseLong( csvRow[11] );

            String languagesString = csvRow[12];
            List<String> languages = (languagesString.isEmpty())
                                     ? EMPTY_LIST
                                     : Lists.newArrayList( collectionSeparatorPattern.split( languagesString, -1 ) );

            String emailsString = csvRow[13];
            List<String> emails = (emailsString.isEmpty())
                                  ? EMPTY_LIST
                                  : Lists.newArrayList( collectionSeparatorPattern.split( emailsString, -1 ) );

            String tagIdsAsString = csvRow[14];
            List<Long> tagIds = new ArrayList<>();
            if ( false == tagIdsAsString.isEmpty() )
            {
                String[] tagIdsAsStrings = collectionSeparatorPattern.split( tagIdsAsString, -1 );
                for ( String tagId : tagIdsAsStrings )
                {
                    tagIds.add( Long.parseLong( tagId ) );
                }
            }

            String studyAtsAsString = csvRow[15];
            List<LdbcUpdate1AddPerson.Organization> studyAts = new ArrayList<>();
            if ( false == studyAtsAsString.isEmpty() )
            {
                String[] studyAtsAsStrings = collectionSeparatorPattern.split( studyAtsAsString, -1 );
                for ( String studyAtAsString : studyAtsAsStrings )
                {
                    String[] studyAtAsStringArray = tupleSeparatorPattern.split( studyAtAsString, -1 );
                    studyAts.add( new LdbcUpdate1AddPerson.Organization(
                                    Long.parseLong( studyAtAsStringArray[0] ),
                                    Integer.parseInt( studyAtAsStringArray[1] )
                            )
                    );
                }
            }

            String worksAtAsString = csvRow[16];
            List<LdbcUpdate1AddPerson.Organization> workAts = new ArrayList<>();
            if ( false == worksAtAsString.isEmpty() )
            {
                String[] workAtsAsStrings = collectionSeparatorPattern.split( worksAtAsString, -1 );
                for ( String workAtAsString : workAtsAsStrings )
                {
                    String[] workAtAsStringArray = tupleSeparatorPattern.split( workAtAsString, -1 );
                    workAts.add( new LdbcUpdate1AddPerson.Organization(
                                    Long.parseLong( workAtAsStringArray[0] ),
                                    Integer.parseInt( workAtAsStringArray[1] )
                            )
                    );
                }
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
                    workAts );
            operation.setScheduledStartTimeAsMilli( scheduledStartTimeAsMilli );
            operation.setTimeStamp( scheduledStartTimeAsMilli );
            operation.setDependencyTimeStamp( dependencyTimeAsMilli );
            return operation;
        }
    }

    public static class EventDecoderAddLikePost implements EventDecoder<Operation>
    {
        public EventDecoderAddLikePost()
        {
        }

        @Override
        public Operation decodeEvent( String[] csvRow )
        {
            long scheduledStartTimeAsMilli = Long.parseLong( csvRow[0] );
            long dependencyTimeAsMilli = Long.parseLong( csvRow[1] );

            long personId = Long.parseLong( csvRow[3] );

            long postId = Long.parseLong( csvRow[4] );

            String creationDateString = csvRow[5];
            Date creationDate = new Date( Long.parseLong( creationDateString ) );

            Operation operation = new LdbcUpdate2AddPostLike( personId, postId, creationDate );
            operation.setScheduledStartTimeAsMilli( scheduledStartTimeAsMilli );
            operation.setTimeStamp( scheduledStartTimeAsMilli );
            operation.setDependencyTimeStamp( dependencyTimeAsMilli );
            return operation;
        }
    }

    public static class EventDecoderAddLikeComment implements EventDecoder<Operation>
    {
        public EventDecoderAddLikeComment()
        {
        }

        @Override
        public Operation decodeEvent( String[] csvRow )
        {
            long scheduledStartTimeAsMilli = Long.parseLong( csvRow[0] );
            long dependencyTimeAsMilli = Long.parseLong( csvRow[1] );

            long personId = Long.parseLong( csvRow[3] );

            long commentId = Long.parseLong( csvRow[4] );

            String creationDateString = csvRow[5];
            Date creationDate = new Date( Long.parseLong( creationDateString ) );

            Operation operation = new LdbcUpdate3AddCommentLike( personId, commentId, creationDate );
            operation.setScheduledStartTimeAsMilli( scheduledStartTimeAsMilli );
            operation.setTimeStamp( scheduledStartTimeAsMilli );
            operation.setDependencyTimeStamp( dependencyTimeAsMilli );
            return operation;
        }
    }

    public static class EventDecoderAddForum implements EventDecoder<Operation>
    {
        private final Pattern collectionSeparatorPattern = Pattern.compile( ";" );

        public EventDecoderAddForum()
        {
        }

        @Override
        public Operation decodeEvent( String[] csvRow )
        {
            long scheduledStartTimeAsMilli = Long.parseLong( csvRow[0] );
            long dependencyTimeAsMilli = Long.parseLong( csvRow[1] );

            long forumId = Long.parseLong( csvRow[3] );

            String forumTitle = csvRow[4];

            String creationDateString = csvRow[5];
            Date creationDate = new Date( Long.parseLong( creationDateString ) );

            long moderatorPersonId = Long.parseLong( csvRow[6] );

            String tagIdsAsString = csvRow[7];
            List<Long> tagIds = new ArrayList<>();
            if ( false == tagIdsAsString.isEmpty() )
            {
                String[] tagIdsAsStrings = collectionSeparatorPattern.split( tagIdsAsString, -1 );
                for ( String tagId : tagIdsAsStrings )
                {
                    tagIds.add( Long.parseLong( tagId ) );
                }
            }

            Operation operation =
                    new LdbcUpdate4AddForum( forumId, forumTitle, creationDate, moderatorPersonId, tagIds );
            operation.setScheduledStartTimeAsMilli( scheduledStartTimeAsMilli );
            operation.setTimeStamp( scheduledStartTimeAsMilli );
            operation.setDependencyTimeStamp( dependencyTimeAsMilli );
            return operation;
        }
    }

    public static class EventDecoderAddForumMembership implements EventDecoder<Operation>
    {
        public EventDecoderAddForumMembership()
        {
        }

        @Override
        public Operation decodeEvent( String[] csvRow )
        {
            long scheduledStartTimeAsMilli = Long.parseLong( csvRow[0] );
            long dependencyTimeAsMilli = Long.parseLong( csvRow[1] );

            long forumId = Long.parseLong( csvRow[3] );

            long personId = Long.parseLong( csvRow[4] );

            String creationDateString = csvRow[5];
            Date creationDate = new Date( Long.parseLong( creationDateString ) );

            Operation operation = new LdbcUpdate5AddForumMembership( forumId, personId, creationDate );
            operation.setScheduledStartTimeAsMilli( scheduledStartTimeAsMilli );
            operation.setTimeStamp( scheduledStartTimeAsMilli );
            operation.setDependencyTimeStamp( dependencyTimeAsMilli );
            return operation;
        }
    }

    public static class EventDecoderAddPost implements EventDecoder<Operation>
    {
        private final Pattern collectionSeparatorPattern = Pattern.compile( ";" );

        public EventDecoderAddPost()
        {
        }

        @Override
        public Operation decodeEvent( String[] csvRow )
        {
            long scheduledStartTimeAsMilli = Long.parseLong( csvRow[0] );
            long dependencyTimeAsMilli = Long.parseLong( csvRow[1] );

            long postId = Long.parseLong( csvRow[3] );

            String imageFile = csvRow[4];

            String creationDateString = csvRow[5];
            Date creationDate = new Date( Long.parseLong( creationDateString ) );

            String locationIp = csvRow[6];

            String browserUsed = csvRow[7];

            String language = csvRow[8];

            String content = csvRow[9];

            int length = Integer.parseInt( csvRow[10] );

            long authorPersonId = Long.parseLong( csvRow[11] );

            long forumId = Long.parseLong( csvRow[12] );

            long countryId = Long.parseLong( csvRow[13] );

            String tagIdsAsString = csvRow[14];
            List<Long> tagIds = new ArrayList<>();
            if ( false == tagIdsAsString.isEmpty() )
            {
                String[] tagIdsAsStrings = collectionSeparatorPattern.split( tagIdsAsString, -1 );
                for ( String tagId : tagIdsAsStrings )
                {
                    tagIds.add( Long.parseLong( tagId ) );
                }
            }

	    String mentionedIdsAsString = csvRow[15];
            List<Long> mentionedIds = new ArrayList<>();
            if ( false == mentionedIdsAsString.isEmpty() )
            {
                String[] mentionedIdsAsStrings = collectionSeparatorPattern.split( mentionedIdsAsString, -1 );
                for ( String mentionedId : mentionedIdsAsStrings )
                {
                    mentionedIds.add( Long.parseLong( mentionedId ) );
                }
            }

	    String privacyString = csvRow[16];
            Boolean privacy = new Boolean( privacyString );

	    String link = csvRow[17];

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
		    link );
            operation.setScheduledStartTimeAsMilli( scheduledStartTimeAsMilli );
            operation.setTimeStamp( scheduledStartTimeAsMilli );
            operation.setDependencyTimeStamp( dependencyTimeAsMilli );
            return operation;
        }
    }

    public static class EventDecoderAddComment implements EventDecoder<Operation>
    {
        private final Pattern collectionSeparatorPattern = Pattern.compile( ";" );

        public EventDecoderAddComment()
        {
        }

        @Override
        public Operation decodeEvent( String[] csvRow )
        {
            long scheduledStartTimeAsMilli = Long.parseLong( csvRow[0] );
            long dependencyTimeAsMilli = Long.parseLong( csvRow[1] );

            long commentId = Long.parseLong( csvRow[3] );

            String creationDateString = csvRow[4];
            Date creationDate = new Date( Long.parseLong( creationDateString ) );

            String locationIp = csvRow[5];

            String browserUsed = csvRow[6];

            String content = csvRow[7];

            int length = Integer.parseInt( csvRow[8] );

            long authorPersonId = Long.parseLong( csvRow[9] );

            long countryId = Long.parseLong( csvRow[10] );

            long replyOfPostId = Long.parseLong( csvRow[11] );

            long replyOfCommentId = Long.parseLong( csvRow[12] );

            String tagIdsAsString = csvRow[13];
            List<Long> tagIds = new ArrayList<>();
            if ( false == tagIdsAsString.isEmpty() )
            {
                String[] tagIdsAsStrings = collectionSeparatorPattern.split( tagIdsAsString, -1 );
                for ( String tagId : tagIdsAsStrings )
                {
                    tagIds.add( Long.parseLong( tagId ) );
                }
            }

	    String mentionedIdsAsString = csvRow[14];
            List<Long> mentionedIds = new ArrayList<>();
            if ( false == mentionedIdsAsString.isEmpty() )
            {
                String[] mentionedIdsAsStrings = collectionSeparatorPattern.split( mentionedIdsAsString, -1 );
                for ( String mentionedId : mentionedIdsAsStrings )
                {
                    mentionedIds.add( Long.parseLong( mentionedId ) );
                }
            }

	    Boolean privacy = Boolean.parseBoolean(csvRow[15]);

	    String link = csvRow[16];

	    String gif = csvRow[17];

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
		    gif );
            operation.setScheduledStartTimeAsMilli( scheduledStartTimeAsMilli );
            operation.setTimeStamp( scheduledStartTimeAsMilli );
            operation.setDependencyTimeStamp( dependencyTimeAsMilli );
            return operation;
        }
    }

    public static class EventDecoderAddFriendship implements EventDecoder<Operation>
    {
        public EventDecoderAddFriendship()
        {
        }

        @Override
        public Operation decodeEvent( String[] csvRow )
        {
            long scheduledStartTimeAsMilli = Long.parseLong( csvRow[0] );
            long dependencyTimeAsMilli = Long.parseLong( csvRow[1] );

            long person1Id = Long.parseLong( csvRow[3] );

            long person2Id = Long.parseLong( csvRow[4] );

            String creationDateString = csvRow[5];
            Date creationDate = new Date( Long.parseLong( creationDateString ) );

            Operation operation = new LdbcUpdate8AddFriendship( person1Id, person2Id, creationDate );
            operation.setScheduledStartTimeAsMilli( scheduledStartTimeAsMilli );
            operation.setTimeStamp( scheduledStartTimeAsMilli );
            operation.setDependencyTimeStamp( dependencyTimeAsMilli );
            return operation;
        }
    }
}

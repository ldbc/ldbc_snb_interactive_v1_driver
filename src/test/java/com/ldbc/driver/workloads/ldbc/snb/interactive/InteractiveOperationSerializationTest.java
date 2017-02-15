package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class InteractiveOperationSerializationTest
{
    private static Kryo kryo = new Kryo();

    private static ByteBufferInput serialize( Operation operation )
    {
        ByteBufferOutput output = new ByteBufferOutput(10000);
        operation.writeKyro( kryo, output );
        return new ByteBufferInput( output.toBytes() );
    }

    @Test
    public void ldbcUpdate1ShouldDoRoundTripCorrectly()
    {
        // Given
        long personId1 = 1;
        String personFirstName1 = "\u16a0";
        String personLastName1 = "\u3055";
        String gender1 = "\u4e35";
        Date birthday1 = new Date( 5 );
        Date creationDate1 = new Date( 6 );
        String locationIp1 = "\u05e4";
        String browserUsed1 = "\u0634";
        long cityId1 = 9;
        List<String> languages1 = Lists.newArrayList( "10" );
        List<String> emails1 = Lists.newArrayList( "11", "12" );
        List<Long> tagIds1 = Lists.newArrayList();
        List<LdbcUpdate1AddPerson.Organization> studyAt1 = Lists.newArrayList( new LdbcUpdate1AddPerson.Organization( 13, 14 ) );
        List<LdbcUpdate1AddPerson.Organization> workAt1 = Lists.newArrayList(
                new LdbcUpdate1AddPerson.Organization( 15, 16 ),
                new LdbcUpdate1AddPerson.Organization( 17, 18 ) );

        long personId2 = 19;
        String personFirstName2 = "20";
        String personLastName2 = "21";
        String gender2 = "22";
        Date birthday2 = new Date( 23 );
        Date creationDate2 = new Date( 24 );
        String locationIp2 = "25";
        String browserUsed2 = "26";
        long cityId2 = 27;
        List<String> languages2 = Lists.newArrayList();
        List<String> emails2 = Lists.newArrayList( "28", "29" );
        List<Long> tagIds2 = Lists.newArrayList( 30l );
        List<LdbcUpdate1AddPerson.Organization> studyAt2 = Lists.newArrayList( new LdbcUpdate1AddPerson.Organization( 31, 32 ) );
        List<LdbcUpdate1AddPerson.Organization> workAt2 = Lists.newArrayList();

        // When
        LdbcUpdate1AddPerson ldbcUpdate1a = new LdbcUpdate1AddPerson(
                personId1,
                personFirstName1,
                personLastName1,
                gender1,
                birthday1,
                creationDate1,
                locationIp1,
                browserUsed1,
                cityId1, languages1, emails1, tagIds1, studyAt1, workAt1 );
        LdbcUpdate1AddPerson ldbcUpdate1b = new LdbcUpdate1AddPerson(
                personId1,
                personFirstName1,
                personLastName1,
                gender1,
                birthday1,
                creationDate1,
                locationIp1,
                browserUsed1,
                cityId1, languages1, emails1, tagIds1, studyAt1, workAt1 );
        LdbcUpdate1AddPerson ldbcUpdate2a = new LdbcUpdate1AddPerson(
                personId2,
                personFirstName2,
                personLastName2,
                gender2,
                birthday2,
                creationDate2,
                locationIp2,
                browserUsed2,
                cityId2, languages2, emails2, tagIds2, studyAt2, workAt2 );
        LdbcUpdate1AddPerson ldbcUpdate2b = new LdbcUpdate1AddPerson(
                personId2,
                personFirstName2,
                personLastName2,
                gender2,
                birthday2,
                creationDate2,
                locationIp2,
                browserUsed2,
                cityId2, languages2, emails2, tagIds2, studyAt2, workAt2 );
        LdbcUpdate1AddPerson ldbcUpdate3a = new LdbcUpdate1AddPerson(
                personId1,
                personFirstName1,
                personLastName1,
                gender1,
                birthday1,
                creationDate1,
                locationIp1,
                browserUsed1,
                cityId1, languages1, emails1, tagIds1, studyAt1, workAt2 );

        // Then
        assertThat( ldbcUpdate1a, equalTo( LdbcUpdate1AddPerson.readKyro( serialize( ldbcUpdate1a ) ) ) );
        assertThat( ldbcUpdate2a, equalTo( LdbcUpdate1AddPerson.readKyro( serialize( ldbcUpdate2a ) ) ) );
        assertThat( ldbcUpdate3a, equalTo( LdbcUpdate1AddPerson.readKyro( serialize( ldbcUpdate3a ) ) ) );
        assertThat( ldbcUpdate1a, not(equalTo( LdbcUpdate1AddPerson.readKyro( serialize( ldbcUpdate2a )) ) ) );
        assertThat( ldbcUpdate2a, not(equalTo( LdbcUpdate1AddPerson.readKyro( serialize( ldbcUpdate1a )) ) ) );
    }

    @Test
    public void ldbcUpdate2ShouldDoRoundTripCorrectly()
    {
        // Given
        long personId1 = 1;
        long postId1 = 2;
        Date creationDate1 = new Date( 3 );

        long personId2 = 4;
        long postId2 = 5;
        Date creationDate2 = new Date( 6 );

        // When
        LdbcUpdate2AddPostLike ldbcUpdate1a = new LdbcUpdate2AddPostLike( personId1, postId1, creationDate1 );
        LdbcUpdate2AddPostLike ldbcUpdate1b = new LdbcUpdate2AddPostLike( personId1, postId1, creationDate1 );
        LdbcUpdate2AddPostLike ldbcUpdate2a = new LdbcUpdate2AddPostLike( personId2, postId2, creationDate2 );
        LdbcUpdate2AddPostLike ldbcUpdate3a = new LdbcUpdate2AddPostLike( personId1, postId1, creationDate2 );

        // Then
        assertThat( ldbcUpdate1a, equalTo( ldbcUpdate1b ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate2a ) ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate3a ) ) );
        assertThat( ldbcUpdate2a, not( equalTo( ldbcUpdate3a ) ) );
    }

    @Test
    public void ldbcUpdate3ShouldDoRoundTripCorrectly()
    {
        // Given
        long personId1 = 1;
        long commentId1 = 2;
        Date creationDate1 = new Date( 3 );

        long personId2 = 4;
        long commentId2 = 5;
        Date creationDate2 = new Date( 6 );

        // When
        LdbcUpdate3AddCommentLike ldbcUpdate1a = new LdbcUpdate3AddCommentLike( personId1, commentId1, creationDate1 );
        LdbcUpdate3AddCommentLike ldbcUpdate1b = new LdbcUpdate3AddCommentLike( personId1, commentId1, creationDate1 );
        LdbcUpdate3AddCommentLike ldbcUpdate2a = new LdbcUpdate3AddCommentLike( personId2, commentId2, creationDate2 );
        LdbcUpdate3AddCommentLike ldbcUpdate3a = new LdbcUpdate3AddCommentLike( personId1, commentId1, creationDate2 );

        // Then
        assertThat( ldbcUpdate1a, equalTo( ldbcUpdate1b ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate2a ) ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate3a ) ) );
        assertThat( ldbcUpdate2a, not( equalTo( ldbcUpdate3a ) ) );
    }

    @Test
    public void ldbcUpdate4ShouldDoRoundTripCorrectly()
    {
        // Given
        long forumId1 = 1;
        String forumTitle1 = "\u16a0";
        Date creationDate1 = new Date( 3 );
        long moderatorPersonId1 = 4;
        List<Long> tagIds1 = Lists.newArrayList( 5l, 6l );

        long forumId2 = 7;
        String forumTitle2 = "\u4e35";
        Date creationDate2 = new Date( 9 );
        long moderatorPersonId2 = 10;
        List<Long> tagIds2 = Lists.newArrayList();

        // When
        LdbcUpdate4AddForum ldbcUpdate1a = new LdbcUpdate4AddForum( forumId1, forumTitle1, creationDate1, moderatorPersonId1, tagIds1 );
        LdbcUpdate4AddForum ldbcUpdate1b = new LdbcUpdate4AddForum( forumId1, forumTitle1, creationDate1, moderatorPersonId1, tagIds1 );
        LdbcUpdate4AddForum ldbcUpdate2a = new LdbcUpdate4AddForum( forumId2, forumTitle2, creationDate2, moderatorPersonId2, tagIds2 );
        LdbcUpdate4AddForum ldbcUpdate3a = new LdbcUpdate4AddForum( forumId1, forumTitle1, creationDate1, moderatorPersonId1, tagIds2 );

        // Then
        assertThat( ldbcUpdate1a, equalTo( ldbcUpdate1b ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate2a ) ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate3a ) ) );
        assertThat( ldbcUpdate2a, not( equalTo( ldbcUpdate3a ) ) );
    }

    @Test
    public void ldbcUpdate5ShouldDoRoundTripCorrectly()
    {
        // Given
        long forumId1 = 1;
        long personId1 = 2;
        Date creationDate1 = new Date( 3 );

        long forumId2 = 4;
        long personId2 = 5;
        Date creationDate2 = new Date( 6 );

        // When
        LdbcUpdate5AddForumMembership ldbcUpdate1a = new LdbcUpdate5AddForumMembership( forumId1, personId1, creationDate1 );
        LdbcUpdate5AddForumMembership ldbcUpdate1b = new LdbcUpdate5AddForumMembership( forumId1, personId1, creationDate1 );
        LdbcUpdate5AddForumMembership ldbcUpdate2a = new LdbcUpdate5AddForumMembership( forumId2, personId2, creationDate2 );
        LdbcUpdate5AddForumMembership ldbcUpdate3a = new LdbcUpdate5AddForumMembership( forumId1, personId1, creationDate2 );

        // Then
        assertThat( ldbcUpdate1a, equalTo( ldbcUpdate1b ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate2a ) ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate3a ) ) );
        assertThat( ldbcUpdate2a, not( equalTo( ldbcUpdate3a ) ) );
    }

    @Test
    public void ldbcUpdate6ShouldDoRoundTripCorrectly()
    {
        // Given
        long postId1 = 1;
        String imageFile1 = "\u16a0";
        Date creationDate1 = new Date( 3 );
        String locationIp1 = "\u4e35";
        String browserUsed1 = "\u05e4";
        String language1 = "\u0634";
        String content1 = "7";
        int length1 = 8;
        long authorPersonId1 = 9;
        long forumId1 = 10;
        long countryId1 = 11;
        List<Long> tagIds1 = Lists.newArrayList( 12l );

        long postId2 = 13;
        String imageFile2 = "14";
        Date creationDate2 = new Date( 15 );
        String locationIp2 = "16";
        String browserUsed2 = "17";
        String language2 = "18";
        String content2 = "19";
        int length2 = 20;
        long authorPersonId2 = 21;
        long forumId2 = 22;
        long countryId2 = 23;
        List<Long> tagIds2 = Lists.newArrayList( 24l, 25l, 26l );

        // When
        LdbcUpdate6AddPost ldbcUpdate1a = new LdbcUpdate6AddPost(
                postId1,
                imageFile1,
                creationDate1,
                locationIp1,
                browserUsed1,
                language1,
                content1,
                length1,
                authorPersonId1,
                forumId1,
                countryId1,
                tagIds1 );

        LdbcUpdate6AddPost ldbcUpdate1b = new LdbcUpdate6AddPost(
                postId1,
                imageFile1,
                creationDate1,
                locationIp1,
                browserUsed1,
                language1,
                content1,
                length1,
                authorPersonId1,
                forumId1,
                countryId1,
                tagIds1 );

        LdbcUpdate6AddPost ldbcUpdate2a = new LdbcUpdate6AddPost(
                postId2,
                imageFile2,
                creationDate2,
                locationIp2,
                browserUsed2,
                language2,
                content2,
                length2,
                authorPersonId2,
                forumId2,
                countryId2,
                tagIds2 );

        LdbcUpdate6AddPost ldbcUpdate3a = new LdbcUpdate6AddPost(
                postId1,
                imageFile1,
                creationDate1,
                locationIp1,
                browserUsed1,
                language1,
                content1,
                length1,
                authorPersonId1,
                forumId1,
                countryId1,
                tagIds2 );


        // Then
        assertThat( ldbcUpdate1a, equalTo( ldbcUpdate1b ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate2a ) ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate3a ) ) );
        assertThat( ldbcUpdate2a, not( equalTo( ldbcUpdate3a ) ) );
    }

    @Test
    public void ldbcUpdate7ShouldDoRoundTripCorrectly()
    {
        // Given
        long commentId1 = 1;
        Date creationDate1 = new Date( 2 );
        String locationIp1 = "\u3055";
        String browserUsed1 = "\u4e35";
        String content1 = "\u05e4";
        int length1 = 6;
        long authorPersonId1 = 7;
        long countryId1 = 8;
        long replyToPostId1 = 9;
        long replyToCommentId1 = 10;
        List<Long> tagIds1 = Lists.newArrayList();

        long commentId2 = 11;
        Date creationDate2 = new Date( 12 );
        String locationIp2 = "\u0634";
        String browserUsed2 = "14";
        String content2 = "15";
        int length2 = 16;
        long authorPersonId2 = 17;
        long countryId2 = 18;
        long replyToPostId2 = 19;
        long replyToCommentId2 = 20;
        List<Long> tagIds2 = Lists.newArrayList( 21l );

        // When
        LdbcUpdate7AddComment ldbcUpdate1a = new LdbcUpdate7AddComment(
                commentId1,
                creationDate1,
                locationIp1,
                browserUsed1,
                content1,
                length1,
                authorPersonId1,
                countryId1,
                replyToPostId1,
                replyToCommentId1,
                tagIds1 );

        LdbcUpdate7AddComment ldbcUpdate1b = new LdbcUpdate7AddComment(
                commentId1,
                creationDate1,
                locationIp1,
                browserUsed1,
                content1,
                length1,
                authorPersonId1,
                countryId1,
                replyToPostId1,
                replyToCommentId1,
                tagIds1 );

        LdbcUpdate7AddComment ldbcUpdate2a = new LdbcUpdate7AddComment(
                commentId2,
                creationDate2,
                locationIp2,
                browserUsed2,
                content2,
                length2,
                authorPersonId2,
                countryId2,
                replyToPostId2,
                replyToCommentId2,
                tagIds2 );

        LdbcUpdate7AddComment ldbcUpdate3a = new LdbcUpdate7AddComment(
                commentId1,
                creationDate1,
                locationIp1,
                browserUsed1,
                content1,
                length1,
                authorPersonId1,
                countryId1,
                replyToPostId1,
                replyToCommentId1,
                tagIds2 );

        // Then
        assertThat( ldbcUpdate1a, equalTo( ldbcUpdate1b ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate2a ) ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate3a ) ) );
        assertThat( ldbcUpdate2a, not( equalTo( ldbcUpdate3a ) ) );
    }

    @Test
    public void ldbcUpdate8ShouldDoRoundTripCorrectly()
    {
        // Given
        long person1Id1 = 1;
        long person2Id1 = 2;
        Date creationDate1 = new Date( 3 );

        long person1Id2 = 4;
        long person2Id2 = 5;
        Date creationDate2 = new Date( 6 );

        // When
        LdbcUpdate8AddFriendship ldbcUpdate1a = new LdbcUpdate8AddFriendship( person1Id1, person2Id1, creationDate1 );
        LdbcUpdate8AddFriendship ldbcUpdate1b = new LdbcUpdate8AddFriendship( person1Id1, person2Id1, creationDate1 );
        LdbcUpdate8AddFriendship ldbcUpdate2a = new LdbcUpdate8AddFriendship( person1Id2, person2Id2, creationDate2 );
        LdbcUpdate8AddFriendship ldbcUpdate3a = new LdbcUpdate8AddFriendship( person1Id1, person2Id1, creationDate2 );

        // Then
        assertThat( ldbcUpdate1a, equalTo( ldbcUpdate1b ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate2a ) ) );
        assertThat( ldbcUpdate1a, not( equalTo( ldbcUpdate3a ) ) );
        assertThat( ldbcUpdate2a, not( equalTo( ldbcUpdate3a ) ) );
    }
}
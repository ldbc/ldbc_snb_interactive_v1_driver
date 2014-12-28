package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class LdbcOperationsSerializeMarshalOperationResultsTest {
    @Test
    public void ldbcQuery1ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery1 operation = DummyLdbcSnbInteractiveOperationInstances.read1();

        long friendId1 = 1;
        String friendLastName1 = "last1";
        int friendDistance1 = 2;
        long friendBirthday1 = 3;
        long friendCreationDate1 = 4;
        String friendGender1 = "ش";
        String friendBrowserUsed1 = "6";
        String friendLocationIp1 = "7";
        Iterable<String> friendEmails1 = Lists.newArrayList("1a", "1b");
        Iterable<String> friendLanguages1 = Lists.newArrayList("1c", "1d");
        String friendCityName1 = "ᚠ";
        Iterable<List<Object>> friendUniversities1 = Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("1e", "1f", "1g"));
        Iterable<List<Object>> friendCompanies1 = Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("1g", "1h", "1i"));
        List<LdbcQuery1Result> before = Lists.newArrayList(new LdbcQuery1Result(
                friendId1,
                friendLastName1,
                friendDistance1,
                friendBirthday1,
                friendCreationDate1,
                friendGender1,
                friendBrowserUsed1,
                friendLocationIp1,
                friendEmails1,
                friendLanguages1,
                friendCityName1,
                friendUniversities1,
                friendCompanies1
        ));

        // When
        List<LdbcQuery1Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery2ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery2 operation = DummyLdbcSnbInteractiveOperationInstances.read2();

        long personId1 = 1;
        String personFirstName1 = "さ";
        String personLastName1 = "丵";
        long postOrCommentId1 = 4;
        String postOrCommentContent1 = "ش";
        long postOrCommentCreationDate1 = 6;
        List<LdbcQuery2Result> before = Lists.newArrayList(new LdbcQuery2Result(
                personId1,
                personFirstName1,
                personLastName1,
                postOrCommentId1,
                postOrCommentContent1,
                postOrCommentCreationDate1));

        // When
        List<LdbcQuery2Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery3ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery3 operation = DummyLdbcSnbInteractiveOperationInstances.read3();

        long personId1 = 1;
        String personFirstName1 = "さ";
        String personLastName1 = "丵";
        int xCount1 = 4;
        int yCount1 = 5;
        int count1 = 6;
        List<LdbcQuery3Result> before = Lists.newArrayList(new LdbcQuery3Result(
                personId1,
                personFirstName1,
                personLastName1,
                xCount1,
                yCount1,
                count1));
        // When
        List<LdbcQuery3Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery4ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery4 operation = DummyLdbcSnbInteractiveOperationInstances.read4();

        String tagName1 = "ᚠ";
        int tagCount1 = 2;
        List<LdbcQuery4Result> before = Lists.newArrayList(new LdbcQuery4Result(
                tagName1,
                tagCount1));

        // When
        List<LdbcQuery4Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery5ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery5 operation = DummyLdbcSnbInteractiveOperationInstances.read5();

        String forumTitle1 = "ᚠ";
        int postCount1 = 2;
        List<LdbcQuery5Result> before = Lists.newArrayList(new LdbcQuery5Result(
                forumTitle1,
                postCount1));

        // When
        List<LdbcQuery5Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery6ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery6 operation = DummyLdbcSnbInteractiveOperationInstances.read6();

        String tagName1 = "ᚠ";
        int tagCount1 = 2;
        List<LdbcQuery6Result> before = Lists.newArrayList(new LdbcQuery6Result(
                tagName1,
                tagCount1));

        // When
        List<LdbcQuery6Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery7ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery7 operation = DummyLdbcSnbInteractiveOperationInstances.read7();

        long personId1 = 1;
        String personFirstName1 = "さ";
        String personLastName1 = "丵";
        long likeCreationDate1 = 4;
        long commentOrPostId1 = 5;
        String commentOrPostContent1 = "6";
        int minutesLatency1 = 7;
        boolean isNew1 = true;
        List<LdbcQuery7Result> before = Lists.newArrayList(new LdbcQuery7Result(
                personId1,
                personFirstName1,
                personLastName1,
                likeCreationDate1,
                commentOrPostId1,
                commentOrPostContent1,
                minutesLatency1,
                isNew1));

        // When
        List<LdbcQuery7Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery8ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery8 operation = DummyLdbcSnbInteractiveOperationInstances.read8();

        long personId1 = 1;
        String personFirstName1 = "さ";
        String personLastName1 = "丵";
        long commentCreationDate1 = 4;
        long commentId1 = 5;
        String commentContent1 = "6";
        List<LdbcQuery8Result> before = Lists.newArrayList(new LdbcQuery8Result(
                personId1,
                personFirstName1,
                personLastName1,
                commentCreationDate1,
                commentId1,
                commentContent1));

        // When
        List<LdbcQuery8Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery9ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery9 operation = DummyLdbcSnbInteractiveOperationInstances.read9();

        long personId1 = 1;
        String personFirstName1 = "さ";
        String personLastName1 = "丵";
        long commentOrPostId1 = 4;
        String commentOrPostContent1 = "ش";
        long commentOrPostCreationDate1 = 6;
        List<LdbcQuery9Result> before = Lists.newArrayList(new LdbcQuery9Result(
                personId1,
                personFirstName1,
                personLastName1,
                commentOrPostId1,
                commentOrPostContent1,
                commentOrPostCreationDate1));

        // When
        List<LdbcQuery9Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery10ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery10 operation = DummyLdbcSnbInteractiveOperationInstances.read10();

        long personId1 = 1;
        String personFirstName1 = "さ";
        String personLastName1 = "丵";
        int commonInterestScore1 = 10;
        String personGender1 = "פ";
        String personCityName1 = "ش";
        List<LdbcQuery10Result> before = Lists.newArrayList(new LdbcQuery10Result(
                personId1,
                personFirstName1,
                personLastName1,
                commonInterestScore1,
                personGender1,
                personCityName1));

        // When
        List<LdbcQuery10Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery11ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery11 operation = DummyLdbcSnbInteractiveOperationInstances.read11();

        long personId1 = 1;
        String personFirstName1 = "さ";
        String personLastName1 = "丵";
        String organizationName1 = "פ";
        int organizationWorkFromYear1 = 5;
        List<LdbcQuery11Result> before = Lists.newArrayList(new LdbcQuery11Result(
                personId1,
                personFirstName1,
                personLastName1,
                organizationName1,
                organizationWorkFromYear1));

        // When
        List<LdbcQuery11Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery12ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery12 operation = DummyLdbcSnbInteractiveOperationInstances.read12();

        long personId1 = 1;
        String personFirstName1 = "さ";
        String personLastName1 = "丵";
        Iterable<String> tagNames1 = Lists.newArrayList("ش", "6");
        int replyCount1 = 7;
        List<LdbcQuery12Result> before = Lists.newArrayList(new LdbcQuery12Result(
                personId1,
                personFirstName1,
                personLastName1,
                tagNames1,
                replyCount1));

        // When
        List<LdbcQuery12Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery13ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery13 operation = DummyLdbcSnbInteractiveOperationInstances.read13();

        int shortestPathLength1 = 1;
        LdbcQuery13Result before = new LdbcQuery13Result(shortestPathLength1);

        // When
        LdbcQuery13Result after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery14ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery14 operation = DummyLdbcSnbInteractiveOperationInstances.read14();

        List<LdbcQuery14Result> before = Lists.newArrayList(new LdbcQuery14Result(Lists.newArrayList(1l, 2l), 3d));

        // When
        List<LdbcQuery14Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcShortQuery1ShouldSerializeAndMarshalLdbcShortQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcShortQuery1PersonProfile operation = DummyLdbcSnbInteractiveOperationInstances.short1();

        LdbcShortQuery1PersonProfileResult before = new LdbcShortQuery1PersonProfileResult("a", "b", 1, "c", "d", 5);

        // When
        LdbcShortQuery1PersonProfileResult after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcShortQuery2ShouldSerializeAndMarshalLdbcShortQuery2Result() throws SerializingMarshallingException {
        // Given
        LdbcShortQuery2PersonPosts operation = DummyLdbcSnbInteractiveOperationInstances.short2();

        List<LdbcShortQuery2PersonPostsResult> before = Lists.newArrayList(
                new LdbcShortQuery2PersonPostsResult(1, "a"),
                new LdbcShortQuery2PersonPostsResult(2, "b")
        );

        // When
        List<LdbcShortQuery2PersonPostsResult> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcShortQuery3ShouldSerializeAndMarshalLdbcShortQuery3Result() throws SerializingMarshallingException {
        // Given
        LdbcShortQuery3PersonFriends operation = DummyLdbcSnbInteractiveOperationInstances.short3();

        List<LdbcShortQuery3PersonFriendsResult> before = Lists.newArrayList(
                new LdbcShortQuery3PersonFriendsResult(1, "a", "b"),
                new LdbcShortQuery3PersonFriendsResult(2, "c", "d")
        );

        // When
        List<LdbcShortQuery3PersonFriendsResult> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcShortQuery4ShouldSerializeAndMarshalLdbcShortQuery4Result() throws SerializingMarshallingException {
        // Given
        LdbcShortQuery4MessageContent operation = DummyLdbcSnbInteractiveOperationInstances.short4();

        LdbcShortQuery4MessageContentResult before = new LdbcShortQuery4MessageContentResult("a");

        // When
        LdbcShortQuery4MessageContentResult after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcShortQuery5ShouldSerializeAndMarshalLdbcShortQuery5Result() throws SerializingMarshallingException {
        // Given
        LdbcShortQuery5MessageCreator operation = DummyLdbcSnbInteractiveOperationInstances.short5();

        LdbcShortQuery5MessageCreatorResult before = new LdbcShortQuery5MessageCreatorResult(1, "a", "b");

        // When
        LdbcShortQuery5MessageCreatorResult after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcShortQuery6ShouldSerializeAndMarshalLdbcShortQuery6Result() throws SerializingMarshallingException {
        // Given
        LdbcShortQuery6MessageForum operation = DummyLdbcSnbInteractiveOperationInstances.short6();

        LdbcShortQuery6MessageForumResult before = new LdbcShortQuery6MessageForumResult(1, "a", 2, "b", "c");

        // When
        LdbcShortQuery6MessageForumResult after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcShortQuery7ShouldSerializeAndMarshalLdbcShortQuery7Result() throws SerializingMarshallingException {
        // Given
        LdbcShortQuery7MessageReplies operation = DummyLdbcSnbInteractiveOperationInstances.short7();

        List<LdbcShortQuery7MessageRepliesResult> before = Lists.newArrayList(
                new LdbcShortQuery7MessageRepliesResult(1, "a"),
                new LdbcShortQuery7MessageRepliesResult(2, "b")
        );

        // When
        List<LdbcShortQuery7MessageRepliesResult> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult(before)
                        )
                )
        );

        // Then
        assertThat(before, equalTo(after));
    }
}

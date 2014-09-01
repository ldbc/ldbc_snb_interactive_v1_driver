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
        String friendGender1 = "5";
        String friendBrowserUsed1 = "6";
        String friendLocationIp1 = "7";
        Iterable<String> friendEmails1 = Lists.newArrayList("1a", "1b");
        Iterable<String> friendLanguages1 = Lists.newArrayList("1c", "1d");
        String friendCityName1 = "1";
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
        String serialized = operation.serializeResult(before);
        List<LdbcQuery1Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery2ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery2 operation = DummyLdbcSnbInteractiveOperationInstances.read2();

        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
        long postOrCommentId1 = 4;
        String postOrCommentContent1 = "5";
        long postOrCommentCreationDate1 = 6;
        List<LdbcQuery2Result> before = Lists.newArrayList(new LdbcQuery2Result(
                personId1,
                personFirstName1,
                personLastName1,
                postOrCommentId1,
                postOrCommentContent1,
                postOrCommentCreationDate1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery2Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery3ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery3 operation = DummyLdbcSnbInteractiveOperationInstances.read3();

        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
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
        String serialized = operation.serializeResult(before);
        List<LdbcQuery3Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery4ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery4 operation = DummyLdbcSnbInteractiveOperationInstances.read4();

        String tagName1 = "1";
        int tagCount1 = 2;
        List<LdbcQuery4Result> before = Lists.newArrayList(new LdbcQuery4Result(
                tagName1,
                tagCount1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery4Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery5ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery5 operation = DummyLdbcSnbInteractiveOperationInstances.read5();

        String forumTitle1 = "1";
        int postCount1 = 2;
        List<LdbcQuery5Result> before = Lists.newArrayList(new LdbcQuery5Result(
                forumTitle1,
                postCount1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery5Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery6ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery6 operation = DummyLdbcSnbInteractiveOperationInstances.read6();

        String tagName1 = "1";
        int tagCount1 = 2;
        List<LdbcQuery6Result> before = Lists.newArrayList(new LdbcQuery6Result(
                tagName1,
                tagCount1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery6Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery7ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery7 operation = DummyLdbcSnbInteractiveOperationInstances.read7();

        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
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
        String serialized = operation.serializeResult(before);
        List<LdbcQuery7Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery8ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery8 operation = DummyLdbcSnbInteractiveOperationInstances.read8();

        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
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
        String serialized = operation.serializeResult(before);
        List<LdbcQuery8Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery9ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery9 operation = DummyLdbcSnbInteractiveOperationInstances.read9();

        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
        long commentOrPostId1 = 4;
        String commentOrPostContent1 = "5";
        long commentOrPostCreationDate1 = 6;
        List<LdbcQuery9Result> before = Lists.newArrayList(new LdbcQuery9Result(
                personId1,
                personFirstName1,
                personLastName1,
                commentOrPostId1,
                commentOrPostContent1,
                commentOrPostCreationDate1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery9Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery10ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery10 operation = DummyLdbcSnbInteractiveOperationInstances.read10();

        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
        int commonInterestScore1 = 10;
        String personGender1 = "4";
        String personCityName1 = "5";
        List<LdbcQuery10Result> before = Lists.newArrayList(new LdbcQuery10Result(
                personId1,
                personFirstName1,
                personLastName1,
                commonInterestScore1,
                personGender1,
                personCityName1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery10Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery11ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery11 operation = DummyLdbcSnbInteractiveOperationInstances.read11();

        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
        String organizationName1 = "4";
        int organizationWorkFromYear1 = 5;
        List<LdbcQuery11Result> before = Lists.newArrayList(new LdbcQuery11Result(
                personId1,
                personFirstName1,
                personLastName1,
                organizationName1,
                organizationWorkFromYear1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery11Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery12ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery12 operation = DummyLdbcSnbInteractiveOperationInstances.read12();

        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
        Iterable<String> tagNames1 = Lists.newArrayList("5", "6");
        int replyCount1 = 7;
        List<LdbcQuery12Result> before = Lists.newArrayList(new LdbcQuery12Result(
                personId1,
                personFirstName1,
                personLastName1,
                tagNames1,
                replyCount1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery12Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery13ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery13 operation = DummyLdbcSnbInteractiveOperationInstances.read13();

        int shortestPathLength1 = 1;
        List<LdbcQuery13Result> before = Lists.newArrayList(new LdbcQuery13Result(shortestPathLength1));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery13Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }

    @Test
    public void ldbcQuery14ShouldSerializeAndMarshalLdbcQuery1Result() throws SerializingMarshallingException {
        // Given
        LdbcQuery14 operation = DummyLdbcSnbInteractiveOperationInstances.read14();

        List<LdbcQuery14Result> before = Lists.newArrayList(new LdbcQuery14Result(Lists.newArrayList(1l, 2l), 3d));

        // When
        String serialized = operation.serializeResult(before);
        List<LdbcQuery14Result> after = operation.marshalResult(serialized);

        // Then
        assertThat(before, equalTo(after));
    }
}

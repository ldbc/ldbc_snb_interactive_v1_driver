package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.OperationTest;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BiReadEventStreamReadersTest
{
    static final GeneratorFactory GENERATOR_FACTORY = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

    @Test
    public void shouldParseAllQuery1Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_1_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery1EventStreamReader reader = new BiQuery1EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery1PostingSummary operation;

        operation = (LdbcSnbBiQuery1PostingSummary) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery1PostingSummary) reader.next();
        assertThat( operation.date(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery1PostingSummary) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery2Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_2_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery2EventStreamReader reader = new BiQuery2EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery2TopTags operation;

        operation = (LdbcSnbBiQuery2TopTags) reader.next();
        assertThat( operation.startDate(), is( 1441351591755l ) );
        assertThat( operation.endDate(), is( 1441351591755l ) );
        assertThat( operation.country1(), is( "countryA" ) );
        assertThat( operation.country2(), is( "countryB" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery2TopTags) reader.next();
        assertThat( operation.startDate(), is( 1441351591755l ) );
        assertThat( operation.endDate(), is( 1441351591755l ) );
        assertThat( operation.country1(), is( "countryA" ) );
        assertThat( operation.country2(), is( "countryC" ) );

        operation = (LdbcSnbBiQuery2TopTags) reader.next();
        assertThat( operation.startDate(), is( 1441351591755l ) );
        assertThat( operation.endDate(), is( 1441351591756l ) );
        assertThat( operation.country1(), is( "countryB" ) );
        assertThat( operation.country2(), is( "countryD" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery2TopTags) reader.next();
        assertThat( operation.startDate(), is( 1441351591755l ) );
        assertThat( operation.endDate(), is( 1441351591755l ) );
        assertThat( operation.country1(), is( "countryA" ) );
        assertThat( operation.country2(), is( "countryB" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery3Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_3_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery3EventStreamReader reader = new BiQuery3EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery3TagEvolution operation;

        operation = (LdbcSnbBiQuery3TagEvolution) reader.next();
        assertThat( operation.year(), is( 1 ) );
        assertThat( operation.month(), is( 2 ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery3TagEvolution) reader.next();
        assertThat( operation.year(), is( 3 ) );
        assertThat( operation.month(), is( 4 ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery3TagEvolution) reader.next();
        assertThat( operation.year(), is( 1 ) );
        assertThat( operation.month(), is( 2 ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery4Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_4_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery4EventStreamReader reader = new BiQuery4EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery4PopularCountryTopics operation;

        operation = (LdbcSnbBiQuery4PopularCountryTopics) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Cameroon" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery4PopularCountryTopics) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Colombia" ) );

        operation = (LdbcSnbBiQuery4PopularCountryTopics) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Niger" ) );

        operation = (LdbcSnbBiQuery4PopularCountryTopics) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Sweden" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery4PopularCountryTopics) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Cameroon" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery5Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_5_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery5EventStreamReader reader = new BiQuery5EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery5TopCountryPosters operation;

        operation = (LdbcSnbBiQuery5TopCountryPosters) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery5TopCountryPosters) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery5TopCountryPosters) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery5TopCountryPosters) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery5TopCountryPosters) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery6Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_6_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery6EventStreamReader reader = new BiQuery6EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery6ActivePosters operation;

        operation = (LdbcSnbBiQuery6ActivePosters) reader.next();
        assertThat( operation.tag(), is( "Justin_Timberlake" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery6ActivePosters) reader.next();
        assertThat( operation.tag(), is( "Josip_Broz_Tito" ) );

        operation = (LdbcSnbBiQuery6ActivePosters) reader.next();
        assertThat( operation.tag(), is( "Barry_Manilow" ) );

        operation = (LdbcSnbBiQuery6ActivePosters) reader.next();
        assertThat( operation.tag(), is( "Charles_Darwin" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery6ActivePosters) reader.next();
        assertThat( operation.tag(), is( "Justin_Timberlake" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery7Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_7_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery7EventStreamReader reader = new BiQuery7EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery7AuthoritativeUsers operation;

        operation = (LdbcSnbBiQuery7AuthoritativeUsers) reader.next();
        assertThat( operation.tag(), is( "Franz_Schubert" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery7AuthoritativeUsers) reader.next();
        assertThat( operation.tag(), is( "Bill_Clinton" ) );

        operation = (LdbcSnbBiQuery7AuthoritativeUsers) reader.next();
        assertThat( operation.tag(), is( "Dante_Alighieri" ) );

        operation = (LdbcSnbBiQuery7AuthoritativeUsers) reader.next();
        assertThat( operation.tag(), is( "Khalid_Sheikh_Mohammed" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery7AuthoritativeUsers) reader.next();
        assertThat( operation.tag(), is( "Franz_Schubert" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery8Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_8_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery8EventStreamReader reader = new BiQuery8EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery8RelatedTopics operation;

        operation = (LdbcSnbBiQuery8RelatedTopics) reader.next();
        assertThat( operation.tag(), is( "Alanis_Morissette" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery8RelatedTopics) reader.next();
        assertThat( operation.tag(), is( "\u00c9amon_de_Valera" ) );

        operation = (LdbcSnbBiQuery8RelatedTopics) reader.next();
        assertThat( operation.tag(), is( "Juhi_Chawla" ) );

        operation = (LdbcSnbBiQuery8RelatedTopics) reader.next();
        assertThat( operation.tag(), is( "Manuel_Noriega" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery8RelatedTopics) reader.next();
        assertThat( operation.tag(), is( "Alanis_Morissette" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery9Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_9_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery9EventStreamReader reader = new BiQuery9EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery9RelatedForums operation;

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClass1(), is( "Person" ) );
        assertThat( operation.tagClass2(), is( "OfficeHolder" ) );
        assertThat( operation.threshold(), is( 1 ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClass1(), is( "Person" ) );
        assertThat( operation.tagClass2(), is( "Writer" ) );
        assertThat( operation.threshold(), is( 2 ) );

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClass1(), is( "Person" ) );
        assertThat( operation.tagClass2(), is( "Single" ) );
        assertThat( operation.threshold(), is( 3 ) );

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClass1(), is( "Person" ) );
        assertThat( operation.tagClass2(), is( "Country" ) );
        assertThat( operation.threshold(), is( 4 ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClass1(), is( "Person" ) );
        assertThat( operation.tagClass2(), is( "OfficeHolder" ) );
        assertThat( operation.threshold(), is( 1 ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery10Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_10_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery10EventStreamReader reader = new BiQuery10EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery10TagPerson operation;

        operation = (LdbcSnbBiQuery10TagPerson) reader.next();
        assertThat( operation.tag(), is( "Franz_Schubert" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery10TagPerson) reader.next();
        assertThat( operation.tag(), is( "Bill_Clinton" ) );

        operation = (LdbcSnbBiQuery10TagPerson) reader.next();
        assertThat( operation.tag(), is( "Dante_Alighieri" ) );

        operation = (LdbcSnbBiQuery10TagPerson) reader.next();
        assertThat( operation.tag(), is( "Khalid_Sheikh_Mohammed" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery10TagPerson) reader.next();
        assertThat( operation.tag(), is( "Franz_Schubert" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery11Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_11_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery11EventStreamReader reader = new BiQuery11EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery11UnrelatedReplies operation;

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Cameroon" ) );
        assertThat( operation.blacklist(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Writer", "Reader" ) ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Colombia" ) );
        assertThat( operation.blacklist(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Writer", "Cake" ) ) );

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Niger" ) );
        assertThat( operation.blacklist(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Pie" ) ) );

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Sweden" ) );
        assertThat( operation.blacklist(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Writer" ) ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Cameroon" ) );
        assertThat( operation.blacklist(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Writer", "Reader" ) ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery12Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_12_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery12EventStreamReader reader = new BiQuery12EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery12TrendingPosts operation;

        operation = (LdbcSnbBiQuery12TrendingPosts) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery12TrendingPosts) reader.next();
        assertThat( operation.date(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery12TrendingPosts) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery13Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_13_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery13EventStreamReader reader = new BiQuery13EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery13PopularMonthlyTags operation;

        operation = (LdbcSnbBiQuery13PopularMonthlyTags) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery13PopularMonthlyTags) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery13PopularMonthlyTags) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery13PopularMonthlyTags) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery13PopularMonthlyTags) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery14Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_14_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery14EventStreamReader reader = new BiQuery14EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery14TopThreadInitiators operation;

        operation = (LdbcSnbBiQuery14TopThreadInitiators) reader.next();
        assertThat( operation.startDate(), is( 1441351591755l ) );
        assertThat( operation.endDate(), is( 1441351591756l ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery14TopThreadInitiators) reader.next();
        assertThat( operation.startDate(), is( 1441351591756l ) );
        assertThat( operation.endDate(), is( 1441351591757l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery14TopThreadInitiators) reader.next();
        assertThat( operation.startDate(), is( 1441351591755l ) );
        assertThat( operation.endDate(), is( 1441351591756l ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery15Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_15_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery15EventStreamReader reader = new BiQuery15EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery15SocialNormals operation;

        operation = (LdbcSnbBiQuery15SocialNormals) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery15SocialNormals) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery15SocialNormals) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery15SocialNormals) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery15SocialNormals) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery16Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_16_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery16EventStreamReader reader = new BiQuery16EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery16ExpertsInSocialCircle operation;

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.personId(), is( 1l ) );
        assertThat( operation.country(), is( "Cameroon" ) );
        assertThat( operation.tagClass(), is( "Writer" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.personId(), is( 2l ) );
        assertThat( operation.country(), is( "Colombia" ) );
        assertThat( operation.tagClass(), is( "Writer" ) );

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.personId(), is( 3l ) );
        assertThat( operation.country(), is( "Niger" ) );
        assertThat( operation.tagClass(), is( "Writer" ) );

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.personId(), is( 4l ) );
        assertThat( operation.country(), is( "Sweden" ) );
        assertThat( operation.tagClass(), is( "Writer" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.personId(), is( 1l ) );
        assertThat( operation.country(), is( "Cameroon" ) );
        assertThat( operation.tagClass(), is( "Writer" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery17Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_17_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery17EventStreamReader reader = new BiQuery17EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery17FriendshipTriangles operation;

        operation = (LdbcSnbBiQuery17FriendshipTriangles) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery17FriendshipTriangles) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery17FriendshipTriangles) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery17FriendshipTriangles) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery17FriendshipTriangles) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery18Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_18_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery18EventStreamReader reader = new BiQuery18EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery18PersonPostCounts operation;

        operation = (LdbcSnbBiQuery18PersonPostCounts) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery18PersonPostCounts) reader.next();
        assertThat( operation.date(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery18PersonPostCounts) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery19Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_19_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery19EventStreamReader reader = new BiQuery19EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery19StrangerInteraction operation;

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 1l ) );
        assertThat( operation.tagClass1(), is( "Writer" ) );
        assertThat( operation.tagClass2(), is( "Single" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 2l ) );
        assertThat( operation.tagClass1(), is( "Writer" ) );
        assertThat( operation.tagClass2(), is( "Country" ) );

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 3l ) );
        assertThat( operation.tagClass1(), is( "Writer" ) );
        assertThat( operation.tagClass2(), is( "Album" ) );

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 4l ) );
        assertThat( operation.tagClass1(), is( "Writer" ) );
        assertThat( operation.tagClass2(), is( "BritishRoyalty" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 1l ) );
        assertThat( operation.tagClass1(), is( "Writer" ) );
        assertThat( operation.tagClass2(), is( "Single" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery20Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_20_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery20EventStreamReader reader = new BiQuery20EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery20HighLevelTopics operation;

        operation = (LdbcSnbBiQuery20HighLevelTopics) reader.next();
        assertThat( operation.tagClasses(), CoreMatchers.<List<String>>is( Lists.newArrayList( "a", "b", "c" ) ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery20HighLevelTopics) reader.next();
        assertThat( operation.tagClasses(), CoreMatchers.<List<String>>is( Lists.newArrayList( "b", "c" ) ) );

        operation = (LdbcSnbBiQuery20HighLevelTopics) reader.next();
        assertThat( operation.tagClasses(), CoreMatchers.<List<String>>is( Lists.newArrayList( "c" ) ) );


        // loops back around to first

        operation = (LdbcSnbBiQuery20HighLevelTopics) reader.next();
        assertThat( operation.tagClasses(), CoreMatchers.<List<String>>is( Lists.newArrayList( "a", "b", "c" ) ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery21Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_21_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery21EventStreamReader reader = new BiQuery21EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery21Zombies operation;

        operation = (LdbcSnbBiQuery21Zombies) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );
        assertThat( operation.endDate(), is( 1l ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery21Zombies) reader.next();
        assertThat( operation.country(), is( "Peru" ) );
        assertThat( operation.endDate(), is( 2l ) );

        operation = (LdbcSnbBiQuery21Zombies) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );
        assertThat( operation.endDate(), is( 3l ) );

        operation = (LdbcSnbBiQuery21Zombies) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );
        assertThat( operation.endDate(), is( 4l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery21Zombies) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );
        assertThat( operation.endDate(), is( 1l ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery22Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_22_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery22EventStreamReader reader = new BiQuery22EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery22InternationalDialog operation;

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.country1(), is( "Germany" ) );
        assertThat( operation.country2(), is( "Pakistan" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.country1(), is( "Germany" ) );
        assertThat( operation.country2(), is( "Russia" ) );

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.country1(), is( "Germany" ) );
        assertThat( operation.country2(), is( "Vietnam" ) );

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.country1(), is( "Germany" ) );
        assertThat( operation.country2(), is( "Philippines" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.country1(), is( "Germany" ) );
        assertThat( operation.country2(), is( "Pakistan" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery23Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_23_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery23EventStreamReader reader = new BiQuery23EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery23HolidayDestinations operation;

        operation = (LdbcSnbBiQuery23HolidayDestinations) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery23HolidayDestinations) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery23HolidayDestinations) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery23HolidayDestinations) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery23HolidayDestinations) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery24Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_24_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery24EventStreamReader reader = new BiQuery24EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery24MessagesByTopic operation;

        operation = (LdbcSnbBiQuery24MessagesByTopic) reader.next();
        assertThat( operation.tagClass(), is( "Person" ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery24MessagesByTopic) reader.next();
        assertThat( operation.tagClass(), is( "OfficeHolder" ) );

        operation = (LdbcSnbBiQuery24MessagesByTopic) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );

        operation = (LdbcSnbBiQuery24MessagesByTopic) reader.next();
        assertThat( operation.tagClass(), is( "Single" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery24MessagesByTopic) reader.next();
        assertThat( operation.tagClass(), is( "Person" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery25Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_25_CSV_ROWS();
        System.out.println( data + "\n" );
        BiQuery25EventStreamReader reader = new BiQuery25EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery25WeightedPaths operation;

        operation = (LdbcSnbBiQuery25WeightedPaths) reader.next();
        assertThat( operation.person1Id(), is( 1L ) );
        assertThat( operation.person2Id(), is( 2L ) );
        assertThat( operation.startDate(), is( 1L ) );
        assertThat( operation.endDate(), is( 2L ) );
        OperationTest.assertCorrectParameterMap(operation);

        operation = (LdbcSnbBiQuery25WeightedPaths) reader.next();
        assertThat( operation.person1Id(), is( 3L ) );
        assertThat( operation.person2Id(), is( 4L ) );
        assertThat( operation.startDate(), is( 3L ) );
        assertThat( operation.endDate(), is( 4L ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery25WeightedPaths) reader.next();
        assertThat( operation.person1Id(), is( 1L ) );
        assertThat( operation.person2Id(), is( 2L ) );
        assertThat( operation.startDate(), is( 1L ) );
        assertThat( operation.endDate(), is( 2L ) );

        assertTrue( reader.hasNext() );
    }

}

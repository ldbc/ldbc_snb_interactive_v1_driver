package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
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
        Query1EventStreamReader reader = new Query1EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery1PostingSummary operation;

        operation = (LdbcSnbBiQuery1PostingSummary) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

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
        Query2EventStreamReader reader = new Query2EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery2TopTags operation;

        operation = (LdbcSnbBiQuery2TopTags) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );
        assertThat( operation.countries(),
                CoreMatchers.<List<String>>equalTo( Lists.newArrayList( "countryA", "countryB" ) ) );
        assertThat( operation.endOfSimulationTime(), is( 1l ) );
        assertThat( operation.messageThreshold(), is( 10 ) );

        operation = (LdbcSnbBiQuery2TopTags) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );
        assertThat( operation.countries(),
                CoreMatchers.<List<String>>equalTo( Lists.newArrayList( "countryA", "countryC" ) ) );
        assertThat( operation.endOfSimulationTime(), is( 2l ) );
        assertThat( operation.messageThreshold(), is( 20 ) );

        operation = (LdbcSnbBiQuery2TopTags) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591756l ) );
        assertThat( operation.countries(),
                CoreMatchers.<List<String>>equalTo( Lists.newArrayList( "countryB", "countryD", "countryE" ) ) );
        assertThat( operation.endOfSimulationTime(), is( Long.MAX_VALUE ) );
        assertThat( operation.messageThreshold(), is( 30 ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery2TopTags) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );
        assertThat( operation.countries(),
                CoreMatchers.<List<String>>equalTo( Lists.newArrayList( "countryA", "countryB" ) ) );
        assertThat( operation.endOfSimulationTime(), is( 1l ) );
        assertThat( operation.messageThreshold(), is( 10 ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery3Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_3_CSV_ROWS();
        System.out.println( data + "\n" );
        Query3EventStreamReader reader = new Query3EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery3TagEvolution operation;

        operation = (LdbcSnbBiQuery3TagEvolution) reader.next();
        assertThat( operation.range1Start(), is( 1l ) );
        assertThat( operation.range1End(), is( 2l ) );
        assertThat( operation.range2Start(), is( 3l ) );
        assertThat( operation.range2End(), is( 4l ) );

        operation = (LdbcSnbBiQuery3TagEvolution) reader.next();
        assertThat( operation.range1Start(), is( 5l ) );
        assertThat( operation.range1End(), is( 6l ) );
        assertThat( operation.range2Start(), is( 7l ) );
        assertThat( operation.range2End(), is( 8l ) );

        operation = (LdbcSnbBiQuery3TagEvolution) reader.next();
        assertThat( operation.range1Start(), is( Long.MAX_VALUE ) );
        assertThat( operation.range1End(), is( Long.MAX_VALUE ) );
        assertThat( operation.range2Start(), is( Long.MAX_VALUE ) );
        assertThat( operation.range2End(), is( Long.MAX_VALUE ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery3TagEvolution) reader.next();
        assertThat( operation.range1Start(), is( 1l ) );
        assertThat( operation.range1End(), is( 2l ) );
        assertThat( operation.range2Start(), is( 3l ) );
        assertThat( operation.range2End(), is( 4l ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery4Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_4_CSV_ROWS();
        System.out.println( data + "\n" );
        Query4EventStreamReader reader = new Query4EventStreamReader(
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
        Query5EventStreamReader reader = new Query5EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery5TopCountryPosters operation;

        operation = (LdbcSnbBiQuery5TopCountryPosters) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

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
        Query6EventStreamReader reader = new Query6EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery6ActivePosters operation;

        operation = (LdbcSnbBiQuery6ActivePosters) reader.next();
        assertThat( operation.tag(), is( "Justin_Timberlake" ) );

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
        Query7EventStreamReader reader = new Query7EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery7AuthoritativeUsers operation;

        operation = (LdbcSnbBiQuery7AuthoritativeUsers) reader.next();
        assertThat( operation.tag(), is( "Franz_Schubert" ) );

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
        Query8EventStreamReader reader = new Query8EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery8RelatedTopics operation;

        operation = (LdbcSnbBiQuery8RelatedTopics) reader.next();
        assertThat( operation.tag(), is( "Alanis_Morissette" ) );

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
        Query9EventStreamReader reader = new Query9EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery9RelatedForums operation;

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "OfficeHolder" ) );
        assertThat( operation.threshold(), is( 1 ) );

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "Writer" ) );
        assertThat( operation.threshold(), is( 2 ) );

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "Single" ) );
        assertThat( operation.threshold(), is( 3 ) );

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "Country" ) );
        assertThat( operation.threshold(), is( 4 ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery9RelatedForums) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "OfficeHolder" ) );
        assertThat( operation.threshold(), is( 1 ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery10Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_10_CSV_ROWS();
        System.out.println( data + "\n" );
        Query10EventStreamReader reader = new Query10EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery10TagPerson operation;

        operation = (LdbcSnbBiQuery10TagPerson) reader.next();
        assertThat( operation.tag(), is( "Franz_Schubert" ) );

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
        Query11EventStreamReader reader = new Query11EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery11UnrelatedReplies operation;

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Cameroon" ) );
        assertThat( operation.blackList(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Writer", "Reader" ) ) );

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Colombia" ) );
        assertThat( operation.blackList(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Writer", "Cake" ) ) );

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Niger" ) );
        assertThat( operation.blackList(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Pie" ) ) );

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Sweden" ) );
        assertThat( operation.blackList(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Writer" ) ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery11UnrelatedReplies) reader.next();
        assertThat( operation.country(), is( "Cameroon" ) );
        assertThat( operation.blackList(), CoreMatchers.<List<String>>is( Lists.newArrayList( "Writer", "Reader" ) ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery12Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_12_CSV_ROWS();
        System.out.println( data + "\n" );
        Query12EventStreamReader reader = new Query12EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery12TrendingPosts operation;

        operation = (LdbcSnbBiQuery12TrendingPosts) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

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
        Query13EventStreamReader reader = new Query13EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery13PopularMonthlyTags operation;

        operation = (LdbcSnbBiQuery13PopularMonthlyTags) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

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
        Query14EventStreamReader reader = new Query14EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery14TopThreadInitiators operation;

        operation = (LdbcSnbBiQuery14TopThreadInitiators) reader.next();
        assertThat( operation.beginDate(), is( 1441351591755l ) );
        assertThat( operation.endDate(), is( 1441351591756l ) );

        operation = (LdbcSnbBiQuery14TopThreadInitiators) reader.next();
        assertThat( operation.beginDate(), is( 1441351591756l ) );
        assertThat( operation.endDate(), is( 1441351591757l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery14TopThreadInitiators) reader.next();
        assertThat( operation.beginDate(), is( 1441351591755l ) );
        assertThat( operation.endDate(), is( 1441351591756l ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery15Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_15_CSV_ROWS();
        System.out.println( data + "\n" );
        Query15EventStreamReader reader = new Query15EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery15SocialNormals operation;

        operation = (LdbcSnbBiQuery15SocialNormals) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

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
        Query16EventStreamReader reader = new Query16EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery16ExpertsInSocialCircle operation;

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.person(), is( 1l ) );
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Cameroon" ) );

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.person(), is( 2l ) );
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Colombia" ) );

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.person(), is( 3l ) );
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Niger" ) );

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.person(), is( 4l ) );
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Sweden" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery16ExpertsInSocialCircle) reader.next();
        assertThat( operation.person(), is( 1l ) );
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Cameroon" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery17Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_17_CSV_ROWS();
        System.out.println( data + "\n" );
        Query17EventStreamReader reader = new Query17EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery17FriendshipTriangles operation;

        operation = (LdbcSnbBiQuery17FriendshipTriangles) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

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
        Query18EventStreamReader reader = new Query18EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery18PersonPostCounts operation;

        operation = (LdbcSnbBiQuery18PersonPostCounts) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

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
        Query19EventStreamReader reader = new Query19EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery19StrangerInteraction operation;

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 1l ) );
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "Single" ) );

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 2l ) );
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "Country" ) );

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 3l ) );
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "Album" ) );

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 4l ) );
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "BritishRoyalty" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery19StrangerInteraction) reader.next();
        assertThat( operation.date(), is( 1l ) );
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "Single" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery20Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_20_CSV_ROWS();
        System.out.println( data + "\n" );
        Query20EventStreamReader reader = new Query20EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery20HighLevelTopics operation;

        operation = (LdbcSnbBiQuery20HighLevelTopics) reader.next();
        assertThat( operation.tagClasses(), CoreMatchers.<List<String>>is( Lists.newArrayList( "a", "b", "c" ) ) );

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
        Query21EventStreamReader reader = new Query21EventStreamReader(
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
        Query22EventStreamReader reader = new Query22EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery22InternationalDialog operation;

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.countryX(), is( "Germany" ) );
        assertThat( operation.countryY(), is( "Pakistan" ) );

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.countryX(), is( "Germany" ) );
        assertThat( operation.countryY(), is( "Russia" ) );

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.countryX(), is( "Germany" ) );
        assertThat( operation.countryY(), is( "Vietnam" ) );

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.countryX(), is( "Germany" ) );
        assertThat( operation.countryY(), is( "Philippines" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery22InternationalDialog) reader.next();
        assertThat( operation.countryX(), is( "Germany" ) );
        assertThat( operation.countryY(), is( "Pakistan" ) );

        assertTrue( reader.hasNext() );
    }

    @Test
    public void shouldParseAllQuery23Events() throws IOException, ParseException, WorkloadException
    {
        // Given
        String data = BiReadEventStreamReadersTestData.QUERY_23_CSV_ROWS();
        System.out.println( data + "\n" );
        Query23EventStreamReader reader = new Query23EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery23HolidayDestinations operation;

        operation = (LdbcSnbBiQuery23HolidayDestinations) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

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
        Query24EventStreamReader reader = new Query24EventStreamReader(
                new ByteArrayInputStream( data.getBytes( Charsets.UTF_8 ) ),
                LdbcSnbBiWorkload.CHAR_SEEKER_PARAMS,
                GENERATOR_FACTORY
        );

        // When

        // Then
        LdbcSnbBiQuery24MessagesByTopic operation;

        operation = (LdbcSnbBiQuery24MessagesByTopic) reader.next();
        assertThat( operation.tagClass(), is( "Person" ) );

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
}

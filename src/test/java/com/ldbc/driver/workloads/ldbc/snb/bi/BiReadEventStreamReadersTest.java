package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.base.Charsets;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;

import static org.hamcrest.CoreMatchers.instanceOf;
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
        LdbcSnbBiQuery1 operation;

        operation = (LdbcSnbBiQuery1) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

        operation = (LdbcSnbBiQuery1) reader.next();
        assertThat( operation.date(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery1) reader.next();
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
        LdbcSnbBiQuery2 operation;

        operation = (LdbcSnbBiQuery2) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );

        operation = (LdbcSnbBiQuery2) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );

        operation = (LdbcSnbBiQuery2) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery2) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );

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
        LdbcSnbBiQuery3 operation;

        operation = (LdbcSnbBiQuery3) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );

        operation = (LdbcSnbBiQuery3) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );

        operation = (LdbcSnbBiQuery3) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery3) reader.next();
        assertThat( operation.dateA(), is( 1441351591755l ) );
        assertThat( operation.dateB(), is( 1441351591755l ) );

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
        LdbcSnbBiQuery4 operation;

        operation = (LdbcSnbBiQuery4) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Cameroon" ) );

        operation = (LdbcSnbBiQuery4) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Colombia" ) );

        operation = (LdbcSnbBiQuery4) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Niger" ) );

        operation = (LdbcSnbBiQuery4) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Sweden" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery4) reader.next();
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
        LdbcSnbBiQuery5 operation;

        operation = (LdbcSnbBiQuery5) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        operation = (LdbcSnbBiQuery5) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery5) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery5) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery5) reader.next();
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
        LdbcSnbBiQuery6 operation;

        operation = (LdbcSnbBiQuery6) reader.next();
        assertThat( operation.tag(), is( "Justin_Timberlake" ) );

        operation = (LdbcSnbBiQuery6) reader.next();
        assertThat( operation.tag(), is( "Josip_Broz_Tito" ) );

        operation = (LdbcSnbBiQuery6) reader.next();
        assertThat( operation.tag(), is( "Barry_Manilow" ) );

        operation = (LdbcSnbBiQuery6) reader.next();
        assertThat( operation.tag(), is( "Charles_Darwin" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery6) reader.next();
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
        LdbcSnbBiQuery7 operation;

        operation = (LdbcSnbBiQuery7) reader.next();
        assertThat( operation.tag(), is( "Franz_Schubert" ) );

        operation = (LdbcSnbBiQuery7) reader.next();
        assertThat( operation.tag(), is( "Bill_Clinton" ) );

        operation = (LdbcSnbBiQuery7) reader.next();
        assertThat( operation.tag(), is( "Dante_Alighieri" ) );

        operation = (LdbcSnbBiQuery7) reader.next();
        assertThat( operation.tag(), is( "Khalid_Sheikh_Mohammed" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery7) reader.next();
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
        LdbcSnbBiQuery8 operation;

        operation = (LdbcSnbBiQuery8) reader.next();
        assertThat( operation.tag(), is( "Alanis_Morissette" ) );

        operation = (LdbcSnbBiQuery8) reader.next();
        assertThat( operation.tag(), is( "Éamon_de_Valera" ) );

        operation = (LdbcSnbBiQuery8) reader.next();
        assertThat( operation.tag(), is( "Juhi_Chawla" ) );

        operation = (LdbcSnbBiQuery8) reader.next();
        assertThat( operation.tag(), is( "Manuel_Noriega" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery8) reader.next();
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
        LdbcSnbBiQuery9 operation;

        operation = (LdbcSnbBiQuery9) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "OfficeHolder" ) );

        operation = (LdbcSnbBiQuery9) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "Writer" ) );

        operation = (LdbcSnbBiQuery9) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "Single" ) );

        operation = (LdbcSnbBiQuery9) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "Country" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery9) reader.next();
        assertThat( operation.tagClassA(), is( "Person" ) );
        assertThat( operation.tagClassB(), is( "OfficeHolder" ) );

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
        LdbcSnbBiQuery10 operation;

        operation = (LdbcSnbBiQuery10) reader.next();
        assertThat( operation.tag(), is( "Franz_Schubert" ) );

        operation = (LdbcSnbBiQuery10) reader.next();
        assertThat( operation.tag(), is( "Bill_Clinton" ) );

        operation = (LdbcSnbBiQuery10) reader.next();
        assertThat( operation.tag(), is( "Dante_Alighieri" ) );

        operation = (LdbcSnbBiQuery10) reader.next();
        assertThat( operation.tag(), is( "Khalid_Sheikh_Mohammed" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery10) reader.next();
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
        LdbcSnbBiQuery11 operation;

        operation = (LdbcSnbBiQuery11) reader.next();
        assertThat( operation.keyWord(), is( "Writer" ) );
        assertThat( operation.country(), is( "Cameroon" ) );

        operation = (LdbcSnbBiQuery11) reader.next();
        assertThat( operation.keyWord(), is( "Writer" ) );
        assertThat( operation.country(), is( "Colombia" ) );

        operation = (LdbcSnbBiQuery11) reader.next();
        assertThat( operation.keyWord(), is( "Writer" ) );
        assertThat( operation.country(), is( "Niger" ) );

        operation = (LdbcSnbBiQuery11) reader.next();
        assertThat( operation.keyWord(), is( "Writer" ) );
        assertThat( operation.country(), is( "Sweden" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery11) reader.next();
        assertThat( operation.keyWord(), is( "Writer" ) );
        assertThat( operation.country(), is( "Cameroon" ) );

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
        LdbcSnbBiQuery12 operation;

        operation = (LdbcSnbBiQuery12) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

        operation = (LdbcSnbBiQuery12) reader.next();
        assertThat( operation.date(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery12) reader.next();
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
        LdbcSnbBiQuery13 operation;

        operation = (LdbcSnbBiQuery13) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        operation = (LdbcSnbBiQuery13) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery13) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery13) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery13) reader.next();
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
        LdbcSnbBiQuery14 operation;

        operation = (LdbcSnbBiQuery14) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

        operation = (LdbcSnbBiQuery14) reader.next();
        assertThat( operation.date(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery14) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

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
        LdbcSnbBiQuery15 operation;

        operation = (LdbcSnbBiQuery15) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        operation = (LdbcSnbBiQuery15) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery15) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery15) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery15) reader.next();
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
        LdbcSnbBiQuery16 operation;

        operation = (LdbcSnbBiQuery16) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Cameroon" ) );

        operation = (LdbcSnbBiQuery16) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Colombia" ) );

        operation = (LdbcSnbBiQuery16) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Niger" ) );

        operation = (LdbcSnbBiQuery16) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );
        assertThat( operation.country(), is( "Sweden" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery16) reader.next();
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
        LdbcSnbBiQuery17 operation;

        operation = (LdbcSnbBiQuery17) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        operation = (LdbcSnbBiQuery17) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery17) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery17) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery17) reader.next();
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
        LdbcSnbBiQuery18 operation;

        operation = (LdbcSnbBiQuery18) reader.next();
        assertThat( operation.date(), is( 1441351591755l ) );

        operation = (LdbcSnbBiQuery18) reader.next();
        assertThat( operation.date(), is( 1441351591756l ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery18) reader.next();
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
        LdbcSnbBiQuery19 operation;

        operation = (LdbcSnbBiQuery19) reader.next();
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "Single" ) );

        operation = (LdbcSnbBiQuery19) reader.next();
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "Country" ) );

        operation = (LdbcSnbBiQuery19) reader.next();
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "Album" ) );

        operation = (LdbcSnbBiQuery19) reader.next();
        assertThat( operation.tagClassA(), is( "Writer" ) );
        assertThat( operation.tagClassB(), is( "BritishRoyalty" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery19) reader.next();
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
        assertThat( reader.next(), instanceOf( LdbcSnbBiQuery20.class ) );
        assertThat( reader.next(), instanceOf( LdbcSnbBiQuery20.class ) );
        assertThat( reader.next(), instanceOf( LdbcSnbBiQuery20.class ) );
        assertThat( reader.next(), instanceOf( LdbcSnbBiQuery20.class )  );

        // loops back around to first

        assertThat( reader.next(), instanceOf( LdbcSnbBiQuery20.class ) );
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
        LdbcSnbBiQuery21 operation;

        operation = (LdbcSnbBiQuery21) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        operation = (LdbcSnbBiQuery21) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery21) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery21) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery21) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

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
        LdbcSnbBiQuery22 operation;

        operation = (LdbcSnbBiQuery22) reader.next();
        assertThat( operation.countryA(), is( "Germany" ) );
        assertThat( operation.countryB(), is( "Pakistan" ) );

        operation = (LdbcSnbBiQuery22) reader.next();
        assertThat( operation.countryA(), is( "Germany" ) );
        assertThat( operation.countryB(), is( "Russia" ) );

        operation = (LdbcSnbBiQuery22) reader.next();
        assertThat( operation.countryA(), is( "Germany" ) );
        assertThat( operation.countryB(), is( "Vietnam" ) );

        operation = (LdbcSnbBiQuery22) reader.next();
        assertThat( operation.countryA(), is( "Germany" ) );
        assertThat( operation.countryB(), is( "Philippines" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery22) reader.next();
        assertThat( operation.countryA(), is( "Germany" ) );
        assertThat( operation.countryB(), is( "Pakistan" ) );

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
        LdbcSnbBiQuery23 operation;

        operation = (LdbcSnbBiQuery23) reader.next();
        assertThat( operation.country(), is( "Kenya" ) );

        operation = (LdbcSnbBiQuery23) reader.next();
        assertThat( operation.country(), is( "Peru" ) );

        operation = (LdbcSnbBiQuery23) reader.next();
        assertThat( operation.country(), is( "Tunisia" ) );

        operation = (LdbcSnbBiQuery23) reader.next();
        assertThat( operation.country(), is( "Venezuela" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery23) reader.next();
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
        LdbcSnbBiQuery24 operation;

        operation = (LdbcSnbBiQuery24) reader.next();
        assertThat( operation.tagClass(), is( "Person" ) );

        operation = (LdbcSnbBiQuery24) reader.next();
        assertThat( operation.tagClass(), is( "OfficeHolder" ) );

        operation = (LdbcSnbBiQuery24) reader.next();
        assertThat( operation.tagClass(), is( "Writer" ) );

        operation = (LdbcSnbBiQuery24) reader.next();
        assertThat( operation.tagClass(), is( "Single" ) );

        // loops back around to first

        operation = (LdbcSnbBiQuery24) reader.next();
        assertThat( operation.tagClass(), is( "Person" ) );

        assertTrue( reader.hasNext() );
    }
}

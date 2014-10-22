//package com.ldbc.driver.workloads.ldbc.snb.interactive;
//
//import com.google.common.collect.ImmutableList;
//import com.ldbc.driver.Operation;
//import com.ldbc.driver.testutils.TestUtils;
//import com.ldbc.driver.util.csv.SimpleCsvFileReader;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.StringReader;
//import java.text.ParseException;
//import java.util.Calendar;
//import java.util.List;
//import java.util.TimeZone;
//
//import static org.hamcrest.CoreMatchers.equalTo;
//import static org.hamcrest.CoreMatchers.is;
//import static org.junit.Assert.assertThat;
//
//public class ReadQueryEventStreamReadersTest {
//    @Test
//    public void shouldParseAllQuery1Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_1_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query1EventStreamReader_OLD reader = new Query1EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery1 operation;
//
//        operation = (LdbcQuery1) reader.next();
//        assertThat(operation.personId(), is(10995117334833L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.firstName(), equalTo("John"));
//
//        operation = (LdbcQuery1) reader.next();
//        assertThat(operation.personId(), is(14293651244033L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.firstName(), equalTo("Yang"));
//
//        operation = (LdbcQuery1) reader.next();
//        assertThat(operation.personId(), is(6597070008725L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.firstName(), equalTo("A."));
//
//        operation = (LdbcQuery1) reader.next();
//        assertThat(operation.personId(), is(2199023331001L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.firstName(), equalTo("Chen"));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery2Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_2_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query2EventStreamReader_OLD reader = new Query2EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
//        LdbcQuery2 operation;
//
//        operation = (LdbcQuery2) reader.next();
//        assertThat(operation.personId(), is(12094628092905L));
//        assertThat(operation.personUri(), equalTo("uri"));
//        calendar.clear();
//        calendar.set(2013, Calendar.JANUARY, 28);
//        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery2) reader.next();
//        assertThat(operation.personId(), is(9895606011404L));
//        assertThat(operation.personUri(), equalTo("uri"));
//        calendar.clear();
//        calendar.set(2013, Calendar.JANUARY, 28);
//        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery2) reader.next();
//        assertThat(operation.personId(), is(14293651244033L));
//        assertThat(operation.personUri(), equalTo("uri"));
//        calendar.clear();
//        calendar.set(2013, Calendar.FEBRUARY, 2);
//        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery2) reader.next();
//        assertThat(operation.personId(), is(13194139602632L));
//        assertThat(operation.personUri(), equalTo("uri"));
//        calendar.clear();
//        calendar.set(2013, Calendar.OCTOBER, 16);
//        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery3Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_3_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query3EventStreamReader_OLD reader = new Query3EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
//        LdbcQuery3 operation;
//
//        operation = (LdbcQuery3) reader.next();
//        assertThat(operation.personId(), is(9895605643992L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.countryXName(), is("Taiwan"));
//        assertThat(operation.countryYName(), is("Bulgaria"));
//        assertThat(operation.durationDays(), is(53));
//        calendar.clear();
//        calendar.set(2011, Calendar.DECEMBER, 1);
//        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery3) reader.next();
//        assertThat(operation.personId(), is(979201L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.countryXName(), is("Nicaragua"));
//        assertThat(operation.countryYName(), is("Afghanistan"));
//        assertThat(operation.durationDays(), is(64));
//        calendar.clear();
//        calendar.set(2012, Calendar.APRIL, 1);
//        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery3) reader.next();
//        assertThat(operation.personId(), is(129891L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.countryXName(), is("Colombia"));
//        assertThat(operation.countryYName(), is("Lithuania"));
//        assertThat(operation.durationDays(), is(58));
//        calendar.clear();
//        calendar.set(2011, Calendar.MAY, 1);
//        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery3) reader.next();
//        assertThat(operation.personId(), is(13194140498760L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.countryXName(), is("Lithuania"));
//        assertThat(operation.countryYName(), is("Afghanistan"));
//        assertThat(operation.durationDays(), is(53));
//        calendar.clear();
//        calendar.set(2010, Calendar.DECEMBER, 1);
//        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery4Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_4_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query4EventStreamReader_OLD reader = new Query4EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
//        LdbcQuery4 operation;
//
//        operation = (LdbcQuery4) reader.next();
//        assertThat(operation.personId(), is(12094628092905L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.durationDays(), is(43));
//        calendar.clear();
//        calendar.set(2011, Calendar.APRIL, 1);
//        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery4) reader.next();
//        assertThat(operation.personId(), is(9895606011404L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.durationDays(), is(36));
//        calendar.clear();
//        calendar.set(2012, Calendar.JANUARY, 1);
//        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery4) reader.next();
//        assertThat(operation.personId(), is(14293651244033L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.durationDays(), is(57));
//        calendar.clear();
//        calendar.set(2011, Calendar.JULY, 1);
//        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery4) reader.next();
//        assertThat(operation.personId(), is(13194139602632L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.durationDays(), is(81));
//        calendar.clear();
//        calendar.set(2011, Calendar.JULY, 1);
//        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery5Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_5_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query5EventStreamReader_OLD reader = new Query5EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
//        LdbcQuery5 operation;
//
//        operation = (LdbcQuery5) reader.next();
//        assertThat(operation.personId(), is(9895605643992L));
//        assertThat(operation.personUri(), is("uri"));
//        calendar.clear();
//        calendar.set(2012, Calendar.DECEMBER, 15);
//        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery5) reader.next();
//        assertThat(operation.personId(), is(979201L));
//        assertThat(operation.personUri(), is("uri"));
//        calendar.clear();
//        calendar.set(2012, Calendar.DECEMBER, 16);
//        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery5) reader.next();
//        assertThat(operation.personId(), is(129891L));
//        assertThat(operation.personUri(), is("uri"));
//        calendar.clear();
//        calendar.set(2012, Calendar.DECEMBER, 14);
//        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery5) reader.next();
//        assertThat(operation.personId(), is(13194140498760L));
//        assertThat(operation.personUri(), is("uri"));
//        calendar.clear();
//        calendar.set(2012, Calendar.DECEMBER, 12);
//        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery6Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_6_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query6EventStreamReader_OLD reader = new Query6EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery6 operation;
//
//        operation = (LdbcQuery6) reader.next();
//        assertThat(operation.personId(), is(9895605643992L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.tagName(), is("Jiang_Zemin"));
//
//        operation = (LdbcQuery6) reader.next();
//        assertThat(operation.personId(), is(979201L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.tagName(), is("Nino_Rota"));
//
//        operation = (LdbcQuery6) reader.next();
//        assertThat(operation.personId(), is(129891L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.tagName(), is("John_VI_of_Portugal"));
//
//        operation = (LdbcQuery6) reader.next();
//        assertThat(operation.personId(), is(13194140498760L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.tagName(), is("Nikolai_Gogol"));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery7Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_7_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query7EventStreamReader_OLD reader = new Query7EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery7 operation;
//
//        operation = (LdbcQuery7) reader.next();
//        assertThat(operation.personId(), is(16492675436774L));
//        assertThat(operation.personUri(), is("uri"));
//
//        operation = (LdbcQuery7) reader.next();
//        assertThat(operation.personId(), is(14293651330072L));
//        assertThat(operation.personUri(), is("uri"));
//
//        operation = (LdbcQuery7) reader.next();
//        assertThat(operation.personId(), is(4398047140913L));
//        assertThat(operation.personUri(), is("uri"));
//
//        operation = (LdbcQuery7) reader.next();
//        assertThat(operation.personId(), is(13194140823804L));
//        assertThat(operation.personUri(), is("uri"));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery8Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_8_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query8EventStreamReader_OLD reader = new Query8EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery8 operation;
//
//        operation = (LdbcQuery8) reader.next();
//        assertThat(operation.personId(), is(15393164184077L));
//        assertThat(operation.personUri(), is("uri"));
//
//        operation = (LdbcQuery8) reader.next();
//        assertThat(operation.personId(), is(15393163594341L));
//        assertThat(operation.personUri(), is("uri"));
//
//        operation = (LdbcQuery8) reader.next();
//        assertThat(operation.personId(), is(7696582593995L));
//        assertThat(operation.personUri(), is("uri"));
//
//        operation = (LdbcQuery8) reader.next();
//        assertThat(operation.personId(), is(15393162809578L));
//        assertThat(operation.personUri(), is("uri"));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery9Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_9_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query9EventStreamReader_OLD reader = new Query9EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
//        LdbcQuery9 operation;
//
//        operation = (LdbcQuery9) reader.next();
//        assertThat(operation.personId(), is(9895605643992L));
//        assertThat(operation.personUri(), is("uri"));
//        calendar.clear();
//        calendar.set(2011, Calendar.DECEMBER, 22);
//        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery9) reader.next();
//        assertThat(operation.personId(), is(979201L));
//        assertThat(operation.personUri(), is("uri"));
//        calendar.clear();
//        calendar.set(2011, Calendar.NOVEMBER, 19);
//        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery9) reader.next();
//        assertThat(operation.personId(), is(129891L));
//        assertThat(operation.personUri(), is("uri"));
//        calendar.clear();
//        calendar.set(2011, Calendar.NOVEMBER, 20);
//        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));
//
//        operation = (LdbcQuery9) reader.next();
//        assertThat(operation.personId(), is(13194140498760L));
//        assertThat(operation.personUri(), is("uri"));
//        calendar.clear();
//        calendar.set(2011, Calendar.DECEMBER, 1);
//        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery10Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_10_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query10EventStreamReader_OLD reader = new Query10EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery10 operation;
//
//        operation = (LdbcQuery10) reader.next();
//        assertThat(operation.personId(), is(9895605643992L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.month(), is(2));
//
//        operation = (LdbcQuery10) reader.next();
//        assertThat(operation.personId(), is(979201L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.month(), is(4));
//
//        operation = (LdbcQuery10) reader.next();
//        assertThat(operation.personId(), is(129891L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.month(), is(2));
//
//        operation = (LdbcQuery10) reader.next();
//        assertThat(operation.personId(), is(13194140498760L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.month(), is(3));
//
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery11Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_11_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query11EventStreamReader_OLD reader = new Query11EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery11 operation;
//
//        operation = (LdbcQuery11) reader.next();
//        assertThat(operation.personId(), is(9895605643992L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.countryName(), is("Taiwan"));
//        assertThat(operation.workFromYear(), is(2013));
//
//        operation = (LdbcQuery11) reader.next();
//        assertThat(operation.personId(), is(979201L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.countryName(), is("Nicaragua"));
//        assertThat(operation.workFromYear(), is(1998));
//
//        operation = (LdbcQuery11) reader.next();
//        assertThat(operation.personId(), is(129891L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.countryName(), is("Colombia"));
//        assertThat(operation.workFromYear(), is(1974));
//
//        operation = (LdbcQuery11) reader.next();
//        assertThat(operation.personId(), is(13194140498760L));
//        assertThat(operation.personUri(), is("uri"));
//        assertThat(operation.countryName(), is("Lithuania"));
//        assertThat(operation.workFromYear(), is(1984));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery12Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_12_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query12EventStreamReader_OLD reader = new Query12EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery12 operation;
//
//        operation = (LdbcQuery12) reader.next();
//        assertThat(operation.personId(), is(12094628092905L));
//        assertThat(operation.personUri(), equalTo("uri"));
//        assertThat(operation.tagClassName(), equalTo("SoccerManager"));
//
//        operation = (LdbcQuery12) reader.next();
//        assertThat(operation.personId(), is(9895606011404L));
//        assertThat(operation.personUri(), equalTo("uri"));
//        assertThat(operation.tagClassName(), equalTo("Chancellor"));
//
//        operation = (LdbcQuery12) reader.next();
//        assertThat(operation.personId(), is(14293651244033L));
//        assertThat(operation.personUri(), equalTo("uri"));
//        assertThat(operation.tagClassName(), equalTo("EurovisionSongContestEntry"));
//
//        operation = (LdbcQuery12) reader.next();
//        assertThat(operation.personId(), is(13194139602632L));
//        assertThat(operation.personUri(), equalTo("uri"));
//        assertThat(operation.tagClassName(), equalTo("GolfPlayer"));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery13Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_13_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query13EventStreamReader_OLD reader = new Query13EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery13 operation;
//
//        operation = (LdbcQuery13) reader.next();
//        assertThat(operation.person1Id(), is(9895605643992L));
//        assertThat(operation.person2Id(), is(1099512323797L));
//        assertThat(operation.person1Uri(), equalTo("uri"));
//        assertThat(operation.person2Uri(), equalTo("uri"));
//
//        operation = (LdbcQuery13) reader.next();
//        assertThat(operation.person1Id(), is(979201L));
//        assertThat(operation.person2Id(), is(95384L));
//        assertThat(operation.person1Uri(), is("uri"));
//        assertThat(operation.person2Uri(), is("uri"));
//
//        operation = (LdbcQuery13) reader.next();
//        assertThat(operation.person1Id(), is(129891L));
//        assertThat(operation.person2Id(), is(9895606000517L));
//        assertThat(operation.person1Uri(), is("uri"));
//        assertThat(operation.person2Uri(), is("uri"));
//
//        operation = (LdbcQuery13) reader.next();
//        assertThat(operation.person1Id(), is(13194140498760L));
//        assertThat(operation.person2Id(), is(7696582276748L));
//        assertThat(operation.person1Uri(), is("uri"));
//        assertThat(operation.person2Uri(), is("uri"));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Test
//    public void shouldParseAllQuery14Events() throws IOException, ParseException {
//        // Given
//        String data = ReadQueryEventStreamReadersTestData.QUERY_14_CSV_ROWS;
//        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
//        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
//        Query14EventStreamReader_OLD reader = new Query14EventStreamReader_OLD(csvFileReader);
//
//        // When
//
//        // Then
//        LdbcQuery14 operation;
//
//        operation = (LdbcQuery14) reader.next();
//        assertThat(operation.person1Id(), is(9895605643992L));
//        assertThat(operation.person2Id(), is(4398046737628L));
//        assertThat(operation.person1Uri(), is("uri"));
//        assertThat(operation.person2Uri(), is("uri"));
//
//        operation = (LdbcQuery14) reader.next();
//        assertThat(operation.person1Id(), is(979201L));
//        assertThat(operation.person2Id(), is(1277748L));
//        assertThat(operation.person1Uri(), is("uri"));
//        assertThat(operation.person2Uri(), is("uri"));
//
//        operation = (LdbcQuery14) reader.next();
//        assertThat(operation.person1Id(), is(129891L));
//        assertThat(operation.person2Id(), is(6597069967720L));
//        assertThat(operation.person1Uri(), is("uri"));
//        assertThat(operation.person2Uri(), is("uri"));
//
//        operation = (LdbcQuery14) reader.next();
//        assertThat(operation.person1Id(), is(13194140498760L));
//        assertThat(operation.person2Id(), is(3298534975254L));
//        assertThat(operation.person1Uri(), is("uri"));
//        assertThat(operation.person2Uri(), is("uri"));
//
//        assertThat(reader.hasNext(), is(false));
//    }
//
//    @Ignore
//    @Test
//    public void shouldParseAllParamsFilesWithoutError() throws IOException, ParseException {
//        // Given
//        Query1EventStreamReader_OLD reader1 = new Query1EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_1_param.txt"), "\\|"));
//        Query2EventStreamReader_OLD reader2 = new Query2EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_2_param.txt"), "\\|"));
//        Query3EventStreamReader_OLD reader3 = new Query3EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_3_param.txt"), "\\|"));
//        Query4EventStreamReader_OLD reader4 = new Query4EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_4_param.txt"), "\\|"));
//        Query5EventStreamReader_OLD reader5 = new Query5EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_5_param.txt"), "\\|"));
//        Query6EventStreamReader_OLD reader6 = new Query6EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_6_param.txt"), "\\|"));
//        Query7EventStreamReader_OLD reader7 = new Query7EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_7_param.txt"), "\\|"));
//        Query8EventStreamReader_OLD reader8 = new Query8EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_8_param.txt"), "\\|"));
//        Query9EventStreamReader_OLD reader9 = new Query9EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_9_param.txt"), "\\|"));
//        Query10EventStreamReader_OLD reader10 = new Query10EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_10_param.txt"), "\\|"));
//        Query11EventStreamReader_OLD reader11 = new Query11EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_11_param.txt"), "\\|"));
//        Query12EventStreamReader_OLD reader12 = new Query12EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_12_param.txt"), "\\|"));
//        Query13EventStreamReader_OLD reader13 = new Query13EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_13_param.txt"), "\\|"));
//        Query14EventStreamReader_OLD reader14 = new Query14EventStreamReader_OLD(new SimpleCsvFileReader(TestUtils.getResource("/query_14_param.txt"), "\\|"));
//
//        // When
//        List<Operation<?>> operation1List = ImmutableList.copyOf(reader1);
//        List<Operation<?>> operation2List = ImmutableList.copyOf(reader2);
//        List<Operation<?>> operation3List = ImmutableList.copyOf(reader3);
//        List<Operation<?>> operation4List = ImmutableList.copyOf(reader4);
//        List<Operation<?>> operation5List = ImmutableList.copyOf(reader5);
//        List<Operation<?>> operation6List = ImmutableList.copyOf(reader6);
//        List<Operation<?>> operation7List = ImmutableList.copyOf(reader7);
//        List<Operation<?>> operation8List = ImmutableList.copyOf(reader8);
//        List<Operation<?>> operation9List = ImmutableList.copyOf(reader9);
//        List<Operation<?>> operation10List = ImmutableList.copyOf(reader10);
//        List<Operation<?>> operation11List = ImmutableList.copyOf(reader11);
//        List<Operation<?>> operation12List = ImmutableList.copyOf(reader12);
//        List<Operation<?>> operation13List = ImmutableList.copyOf(reader13);
//        List<Operation<?>> operation14List = ImmutableList.copyOf(reader14);
//
//        // Then
//        assertThat(operation1List.size() > 1, is(true));
//        assertThat(operation2List.size() > 1, is(true));
//        assertThat(operation3List.size() > 1, is(true));
//        assertThat(operation4List.size() > 1, is(true));
//        assertThat(operation5List.size() > 1, is(true));
//        assertThat(operation6List.size() > 1, is(true));
//        assertThat(operation7List.size() > 1, is(true));
//        assertThat(operation8List.size() > 1, is(true));
//        assertThat(operation9List.size() > 1, is(true));
//        assertThat(operation10List.size() > 1, is(true));
//        assertThat(operation11List.size() > 1, is(true));
//        assertThat(operation12List.size() > 1, is(true));
//        assertThat(operation13List.size() > 1, is(true));
//        assertThat(operation14List.size() > 1, is(true));
//    }
//}

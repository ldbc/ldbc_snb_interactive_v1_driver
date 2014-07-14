package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.Operation;
import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.TestUtils;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ReadQueryEventStreamReadersTest {
    @Test
    public void shouldParseAllQuery1Events() throws IOException, ParseException {
        // Given
        Query1EventStreamReader reader = new Query1EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_1_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery1 operation;

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.personId(), is(10995117334833L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers10995117334833"));
        assertThat(operation.firstName(), equalTo("John"));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.personId(), is(14293651244033L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers14293651244033"));
        assertThat(operation.firstName(), equalTo("Yang"));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.personId(), is(6597070008725L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers6597070008725"));
        assertThat(operation.firstName(), equalTo("A."));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.personId(), is(2199023331001L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers2199023331001"));
        assertThat(operation.firstName(), equalTo("Chen"));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery2Events() throws IOException, ParseException {
        // Given
        Query2EventStreamReader reader = new Query2EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_2_param.txt"), "\\|"));

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        LdbcQuery2 operation;

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.personId(), is(12094628092905L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers12094628092905"));
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 28);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.personId(), is(9895606011404L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895606011404"));
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 28);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.personId(), is(14293651244033L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers14293651244033"));
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 28);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.personId(), is(13194139602632L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194139602632"));
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 28);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery3Events() throws IOException, ParseException {
        // Given
        Query3EventStreamReader reader = new Query3EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_3_param.txt"), "\\|"));

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        LdbcQuery3 operation;

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895605643992"));
        assertThat(operation.countryXName(), is("Taiwan"));
        assertThat(operation.countryYName(), is("Bulgaria"));
        assertThat(operation.durationDays(), is(53));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers979201"));
        assertThat(operation.countryXName(), is("Nicaragua"));
        assertThat(operation.countryYName(), is("Afghanistan"));
        assertThat(operation.durationDays(), is(64));
        calendar.clear();
        calendar.set(2012, Calendar.APRIL, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers129891"));
        assertThat(operation.countryXName(), is("Colombia"));
        assertThat(operation.countryYName(), is("Lithuania"));
        assertThat(operation.durationDays(), is(58));
        calendar.clear();
        calendar.set(2011, Calendar.MAY, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140498760"));
        assertThat(operation.countryXName(), is("Lithuania"));
        assertThat(operation.countryYName(), is("Afghanistan"));
        assertThat(operation.durationDays(), is(53));
        calendar.clear();
        calendar.set(2010, Calendar.DECEMBER, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(true));
    }


    @Test
    public void shouldParseAllQuery4Events() throws IOException, ParseException {
        // Given
        Query4EventStreamReader reader = new Query4EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_4_param.txt"), "\\|"));

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        LdbcQuery4 operation;

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.personId(), is(12094628092905L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers12094628092905"));
        assertThat(operation.durationDays(), is(43));
        calendar.clear();
        calendar.set(2011, Calendar.APRIL, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.personId(), is(9895606011404L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895606011404"));
        assertThat(operation.durationDays(), is(36));
        calendar.clear();
        calendar.set(2012, Calendar.JANUARY, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.personId(), is(14293651244033L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers14293651244033"));
        assertThat(operation.durationDays(), is(57));
        calendar.clear();
        calendar.set(2011, Calendar.JULY, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.personId(), is(13194139602632L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194139602632"));
        assertThat(operation.durationDays(), is(81));
        calendar.clear();
        calendar.set(2011, Calendar.JULY, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery5Events() throws IOException, ParseException {
        // Given
        Query5EventStreamReader reader = new Query5EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_5_param.txt"), "\\|"));

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        LdbcQuery5 operation;

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895605643992"));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 15);
        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers979201"));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 16);
        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers129891"));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 14);
        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140498760"));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 12);
        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery6Events() throws IOException, ParseException {
        // Given
        Query6EventStreamReader reader = new Query6EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_6_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery6 operation;

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895605643992"));
        assertThat(operation.tagName(), is("Jiang_Zemin"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers979201"));
        assertThat(operation.tagName(), is("Nino_Rota"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers129891"));
        assertThat(operation.tagName(), is("John_VI_of_Portugal"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140498760"));
        assertThat(operation.tagName(), is("Nikolai_Gogol"));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery7Events() throws IOException, ParseException {
        // Given
        Query7EventStreamReader reader = new Query7EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_7_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery7 operation;

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.personId(), is(16492675436774L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers16492675436774"));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.personId(), is(14293651330072L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers14293651330072"));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.personId(), is(4398047140913L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers4398047140913"));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.personId(), is(13194140823804L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140823804"));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery8Events() throws IOException, ParseException {
        // Given
        Query8EventStreamReader reader = new Query8EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_8_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery8 operation;

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.personId(), is(15393164184077L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers15393164184077"));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.personId(), is(15393163594341L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers15393163594341"));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.personId(), is(7696582593995L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers7696582593995"));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.personId(), is(15393162809578L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers15393162809578"));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery9Events() throws IOException, ParseException {
        // Given
        Query9EventStreamReader reader = new Query9EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_9_param.txt"), "\\|"));

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        LdbcQuery9 operation;

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895605643992"));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 22);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers979201"));
        calendar.clear();
        calendar.set(2011, Calendar.NOVEMBER, 19);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers129891"));
        calendar.clear();
        calendar.set(2011, Calendar.NOVEMBER, 20);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140498760"));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 1);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery10Events() throws IOException, ParseException {
        // Given
        Query10EventStreamReader reader = new Query10EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_10_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery10 operation;

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895605643992"));
        assertThat(operation.month1(), is(2));
        assertThat(operation.month2(), is(3));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers979201"));
        assertThat(operation.month1(), is(4));
        assertThat(operation.month2(), is(5));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers129891"));
        assertThat(operation.month1(), is(2));
        assertThat(operation.month2(), is(3));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140498760"));
        assertThat(operation.month1(), is(3));
        assertThat(operation.month2(), is(4));


        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery11Events() throws IOException, ParseException {
        // Given
        Query11EventStreamReader reader = new Query11EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_11_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery11 operation;

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895605643992"));
        assertThat(operation.countryName(), is("Taiwan"));
        assertThat(operation.workFromYear(), is(2013));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers979201"));
        assertThat(operation.countryName(), is("Nicaragua"));
        assertThat(operation.workFromYear(), is(1998));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers129891"));
        assertThat(operation.countryName(), is("Colombia"));
        assertThat(operation.workFromYear(), is(1974));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.personUri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140498760"));
        assertThat(operation.countryName(), is("Lithuania"));
        assertThat(operation.workFromYear(), is(1984));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery12Events() throws IOException, ParseException {
        // Given
        Query12EventStreamReader reader = new Query12EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_12_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery12 operation;

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.personId(), is(12094628092905L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers12094628092905"));
        assertThat(operation.tagClassName(), equalTo("SoccerManager"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.personId(), is(9895606011404L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895606011404"));
        assertThat(operation.tagClassName(), equalTo("Chancellor"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.personId(), is(14293651244033L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers14293651244033"));
        assertThat(operation.tagClassName(), equalTo("EurovisionSongContestEntry"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.personId(), is(13194139602632L));
        assertThat(operation.personUri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194139602632"));
        assertThat(operation.tagClassName(), equalTo("GolfPlayer"));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery13Events() throws IOException, ParseException {
        // Given
        Query13EventStreamReader reader = new Query13EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_13_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery13 operation;

        operation = (LdbcQuery13) reader.next();
        assertThat(operation.person1Id(), is(9895605643992L));
        assertThat(operation.person2Id(), is(1099512323797L));
        assertThat(operation.person1Uri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895605643992"));
        assertThat(operation.person2Uri(), equalTo("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers1099512323797"));

        operation = (LdbcQuery13) reader.next();
        assertThat(operation.person1Id(), is(979201L));
        assertThat(operation.person2Id(), is(95384L));
        assertThat(operation.person1Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers979201"));
        assertThat(operation.person2Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers95384"));

        operation = (LdbcQuery13) reader.next();
        assertThat(operation.person1Id(), is(129891L));
        assertThat(operation.person2Id(), is(9895606000517L));
        assertThat(operation.person1Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers129891"));
        assertThat(operation.person2Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895606000517"));

        operation = (LdbcQuery13) reader.next();
        assertThat(operation.person1Id(), is(13194140498760L));
        assertThat(operation.person2Id(), is(7696582276748L));
        assertThat(operation.person1Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140498760"));
        assertThat(operation.person2Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers7696582276748"));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllQuery14Events() throws IOException, ParseException {
        // Given
        Query14EventStreamReader reader = new Query14EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_14_param.txt"), "\\|"));

        // When

        // Then
        LdbcQuery14 operation;

        operation = (LdbcQuery14) reader.next();
        assertThat(operation.person1Id(), is(9895605643992L));
        assertThat(operation.person2Id(), is(4398046737628L));
        assertThat(operation.person1Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers9895605643992"));
        assertThat(operation.person2Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers4398046737628"));

        operation = (LdbcQuery14) reader.next();
        assertThat(operation.person1Id(), is(979201L));
        assertThat(operation.person2Id(), is(1277748L));
        assertThat(operation.person1Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers979201"));
        assertThat(operation.person2Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers1277748"));

        operation = (LdbcQuery14) reader.next();
        assertThat(operation.person1Id(), is(129891L));
        assertThat(operation.person2Id(), is(6597069967720L));
        assertThat(operation.person1Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers129891"));
        assertThat(operation.person2Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers6597069967720"));

        operation = (LdbcQuery14) reader.next();
        assertThat(operation.person1Id(), is(13194140498760L));
        assertThat(operation.person2Id(), is(3298534975254L));
        assertThat(operation.person1Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers13194140498760"));
        assertThat(operation.person2Uri(), is("http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers3298534975254"));

        assertThat(reader.hasNext(), is(true));
    }

    @Test
    public void shouldParseAllParamsFilesWithoutError() throws IOException, ParseException {
        // Given
        Query1EventStreamReader reader1 = new Query1EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_1_param.txt"), "\\|"));
        Query2EventStreamReader reader2 = new Query2EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_2_param.txt"), "\\|"));
        Query3EventStreamReader reader3 = new Query3EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_3_param.txt"), "\\|"));
        Query4EventStreamReader reader4 = new Query4EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_4_param.txt"), "\\|"));
        Query5EventStreamReader reader5 = new Query5EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_5_param.txt"), "\\|"));
        Query6EventStreamReader reader6 = new Query6EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_6_param.txt"), "\\|"));
        Query7EventStreamReader reader7 = new Query7EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_7_param.txt"), "\\|"));
        Query8EventStreamReader reader8 = new Query8EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_8_param.txt"), "\\|"));
        Query9EventStreamReader reader9 = new Query9EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_9_param.txt"), "\\|"));
        Query10EventStreamReader reader10 = new Query10EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_10_param.txt"), "\\|"));
        Query11EventStreamReader reader11 = new Query11EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_11_param.txt"), "\\|"));
        Query12EventStreamReader reader12 = new Query12EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_12_param.txt"), "\\|"));
        Query13EventStreamReader reader13 = new Query13EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_13_param.txt"), "\\|"));
        Query14EventStreamReader reader14 = new Query14EventStreamReader(new CsvFileReader(TestUtils.getResource("/query_14_param.txt"), "\\|"));

        // When
        List<Operation<?>> operation1List = ImmutableList.copyOf(reader1);
        List<Operation<?>> operation2List = ImmutableList.copyOf(reader2);
        List<Operation<?>> operation3List = ImmutableList.copyOf(reader3);
        List<Operation<?>> operation4List = ImmutableList.copyOf(reader4);
        List<Operation<?>> operation5List = ImmutableList.copyOf(reader5);
        List<Operation<?>> operation6List = ImmutableList.copyOf(reader6);
        List<Operation<?>> operation7List = ImmutableList.copyOf(reader7);
        List<Operation<?>> operation8List = ImmutableList.copyOf(reader8);
        List<Operation<?>> operation9List = ImmutableList.copyOf(reader9);
        List<Operation<?>> operation10List = ImmutableList.copyOf(reader10);
        List<Operation<?>> operation11List = ImmutableList.copyOf(reader11);
        List<Operation<?>> operation12List = ImmutableList.copyOf(reader12);
        List<Operation<?>> operation13List = ImmutableList.copyOf(reader13);
        List<Operation<?>> operation14List = ImmutableList.copyOf(reader14);

        // Then
        assertThat(operation1List.size() > 1, is(true));
        assertThat(operation2List.size() > 1, is(true));
        assertThat(operation3List.size() > 1, is(true));
        assertThat(operation4List.size() > 1, is(true));
        assertThat(operation5List.size() > 1, is(true));
        assertThat(operation6List.size() > 1, is(true));
        assertThat(operation7List.size() > 1, is(true));
        assertThat(operation8List.size() > 1, is(true));
        assertThat(operation9List.size() > 1, is(true));
        assertThat(operation10List.size() > 1, is(true));
        assertThat(operation11List.size() > 1, is(true));
        assertThat(operation12List.size() > 1, is(true));
        assertThat(operation13List.size() > 1, is(true));
        assertThat(operation14List.size() > 1, is(true));
    }
}

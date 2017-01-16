package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.csv.charseeker.*;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class InteractiveReadEventStreamReadersTest
{
    @Test
    public void shouldParseAllQuery1Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_1_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query1EventStreamReader.Query1Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query1EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery1 operation;

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.personId(), is(10995117334833L));
        assertThat(operation.firstName(), equalTo("John"));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.personId(), is(14293651244033L));
        assertThat(operation.firstName(), equalTo("Yang"));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.personId(), is(6597070008725L));
        assertThat(operation.firstName(), equalTo("A."));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.personId(), is(2199023331001L));
        assertThat(operation.firstName(), equalTo("Chen"));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery2Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_2_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query2EventStreamReader.Query2Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query2EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        LdbcQuery2 operation;

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.personId(), is(12094628092905L));
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 28);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.personId(), is(9895606011404L));
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 28);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.personId(), is(14293651244033L));
        calendar.clear();
        calendar.set(2013, Calendar.FEBRUARY, 2);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.personId(), is(13194139602632L));
        calendar.clear();
        calendar.set(2013, Calendar.OCTOBER, 16);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery3Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_3_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query3EventStreamReader.Query3Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query3EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        LdbcQuery3 operation;

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.countryXName(), is("Taiwan"));
        assertThat(operation.countryYName(), is("Bulgaria"));
        assertThat(operation.durationDays(), is(53));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.countryXName(), is("Nicaragua"));
        assertThat(operation.countryYName(), is("Afghanistan"));
        assertThat(operation.durationDays(), is(64));
        calendar.clear();
        calendar.set(2012, Calendar.APRIL, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.countryXName(), is("Colombia"));
        assertThat(operation.countryYName(), is("Lithuania"));
        assertThat(operation.durationDays(), is(58));
        calendar.clear();
        calendar.set(2011, Calendar.MAY, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.countryXName(), is("Lithuania"));
        assertThat(operation.countryYName(), is("Afghanistan"));
        assertThat(operation.durationDays(), is(53));
        calendar.clear();
        calendar.set(2010, Calendar.DECEMBER, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery4Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_4_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query4EventStreamReader.Query4Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query4EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        LdbcQuery4 operation;

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.personId(), is(12094628092905L));
        assertThat(operation.durationDays(), is(43));
        calendar.clear();
        calendar.set(2011, Calendar.APRIL, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.personId(), is(9895606011404L));
        assertThat(operation.durationDays(), is(36));
        calendar.clear();
        calendar.set(2012, Calendar.JANUARY, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.personId(), is(14293651244033L));
        assertThat(operation.durationDays(), is(57));
        calendar.clear();
        calendar.set(2011, Calendar.JULY, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.personId(), is(13194139602632L));
        assertThat(operation.durationDays(), is(81));
        calendar.clear();
        calendar.set(2011, Calendar.JULY, 1);
        assertThat(operation.startDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery5Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_5_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query5EventStreamReader.Query5Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query5EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        LdbcQuery5 operation;

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 15);
        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.personId(), is(979201L));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 16);
        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.personId(), is(129891L));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 14);
        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 12);
        assertThat(operation.minDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery6Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_6_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query6EventStreamReader.Query6Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query6EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery6 operation;

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.tagName(), is("Jiang_Zemin"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.tagName(), is("Nino_Rota"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.tagName(), is("John_VI_of_Portugal"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.tagName(), is("Nikolai_Gogol"));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery7Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_7_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query7EventStreamReader.Query7Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query7EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery7 operation;

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.personId(), is(16492675436774L));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.personId(), is(14293651330072L));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.personId(), is(4398047140913L));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.personId(), is(13194140823804L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery8Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_8_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query8EventStreamReader.Query8Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query8EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery8 operation;

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.personId(), is(15393164184077L));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.personId(), is(15393163594341L));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.personId(), is(7696582593995L));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.personId(), is(15393162809578L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery9Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_9_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query9EventStreamReader.Query9Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query9EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        LdbcQuery9 operation;

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 22);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.personId(), is(979201L));
        calendar.clear();
        calendar.set(2011, Calendar.NOVEMBER, 19);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.personId(), is(129891L));
        calendar.clear();
        calendar.set(2011, Calendar.NOVEMBER, 20);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 1);
        assertThat(operation.maxDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery10Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_10_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query10EventStreamReader.Query10Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query10EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery10 operation;

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.month(), is(2));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.month(), is(4));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.month(), is(2));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.month(), is(3));


        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery11Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_11_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query11EventStreamReader.Query11Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query11EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery11 operation;

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.personId(), is(9895605643992L));
        assertThat(operation.countryName(), is("Taiwan"));
        assertThat(operation.workFromYear(), is(2013));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.personId(), is(979201L));
        assertThat(operation.countryName(), is("Nicaragua"));
        assertThat(operation.workFromYear(), is(1998));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.personId(), is(129891L));
        assertThat(operation.countryName(), is("Colombia"));
        assertThat(operation.workFromYear(), is(1974));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.personId(), is(13194140498760L));
        assertThat(operation.countryName(), is("Lithuania"));
        assertThat(operation.workFromYear(), is(1984));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery12Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_12_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query12EventStreamReader.Query12Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query12EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery12 operation;

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.personId(), is(12094628092905L));
        assertThat(operation.tagClassName(), equalTo("SoccerManager"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.personId(), is(9895606011404L));
        assertThat(operation.tagClassName(), equalTo("Chancellor"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.personId(), is(14293651244033L));
        assertThat(operation.tagClassName(), equalTo("EurovisionSongContestEntry"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.personId(), is(13194139602632L));
        assertThat(operation.tagClassName(), equalTo("GolfPlayer"));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery13Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_13_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query13EventStreamReader.Query13Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query13EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery13 operation;

        operation = (LdbcQuery13) reader.next();
//        assertThat(operation.person1Id(), is(9895605643992L));
//        assertThat(operation.person2Id(), is(1099512323797L));
//
//        operation = (LdbcQuery13) reader.next();
//        assertThat(operation.person1Id(), is(979201L));
//        assertThat(operation.person2Id(), is(95384L));
//
//        operation = (LdbcQuery13) reader.next();
//        assertThat(operation.person1Id(), is(129891L));
//        assertThat(operation.person2Id(), is(9895606000517L));
//
//        operation = (LdbcQuery13) reader.next();
//        assertThat(operation.person1Id(), is(13194140498760L));
//        assertThat(operation.person2Id(), is(7696582276748L));
//
//        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery14Events() throws IOException, ParseException {
        // Given
        String data = InteractiveReadEventStreamReadersTestData.QUERY_14_CSV_ROWS();
        System.out.println(data + "\n");
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';', ',');
        CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query14EventStreamReader.Query14Decoder();
        Mark mark = new Mark();
        Iterator<Operation> reader = new Query14EventStreamReader(
                new CsvEventStreamReaderBasicCharSeeker<>(
                        charSeeker,
                        extractors,
                        mark,
                        decoder,
                        columnDelimiter
                )
        );

        // When

        // Then
        LdbcQuery14 operation;

        operation = (LdbcQuery14) reader.next();
//        assertThat(operation.person1Id(), is(9895605643992L));
//        assertThat(operation.person2Id(), is(4398046737628L));
//
//        operation = (LdbcQuery14) reader.next();
//        assertThat(operation.person1Id(), is(979201L));
//        assertThat(operation.person2Id(), is(1277748L));
//
//        operation = (LdbcQuery14) reader.next();
//        assertThat(operation.person1Id(), is(129891L));
//        assertThat(operation.person2Id(), is(6597069967720L));
//
//        operation = (LdbcQuery14) reader.next();
//        assertThat(operation.person1Id(), is(13194140498760L));
//        assertThat(operation.person2Id(), is(3298534975254L));
//
//        assertThat(reader.hasNext(), is(false));
    }
}

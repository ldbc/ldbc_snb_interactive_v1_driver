package org.ldbcouncil.snb.driver.workloads.interactive;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.csv.ParquetLoader;
import org.ldbcouncil.snb.driver.csv.DuckDbConnectionState;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class InteractiveReadEventStreamReadersTest
{
    private DuckDbConnectionState db;
    private Statement stmt;

    /**
     * Initialize mock objects used in all the tests
     * @throws SQLException
     */
    @BeforeEach
    public void init() throws SQLException {
        Connection connection = mock(Connection.class);
        db = mock(DuckDbConnectionState.class);
        when(db.getConnection()).thenReturn(connection);
        stmt = mock(Statement.class);
        when(connection.createStatement()).thenReturn(stmt);
    }

    @Test
    public void shouldParseAllQuery1Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getString(2))
            .thenReturn("John")
            .thenReturn("Yang")
            .thenReturn("A.")
            .thenReturn("Chen");
        when(rs.getLong(1))
            .thenReturn(10995117334833l)
            .thenReturn(14293651244033l)
            .thenReturn(6597070008725l)
            .thenReturn(2199023331001l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query1Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery1 operation;

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.getPersonIdQ1(), is(10995117334833L));
        assertThat(operation.getFirstName(), equalTo("John"));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.getPersonIdQ1(), is(14293651244033L));
        assertThat(operation.getFirstName(), equalTo("Yang"));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.getPersonIdQ1(), is(6597070008725L));
        assertThat(operation.getFirstName(), equalTo("A."));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.getPersonIdQ1(), is(2199023331001L));
        assertThat(operation.getFirstName(), equalTo("Chen"));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery2Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(12094628092905l)
            .thenReturn(9895606011404l)
            .thenReturn(14293651244033l)
            .thenReturn(13194139602632l);
        when(rs.getString(2))
            .thenReturn("2013-01-28")
            .thenReturn("2013-01-28")
            .thenReturn("2013-02-02")
            .thenReturn("2013-10-16");
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query2Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        LdbcQuery2 operation;
        // Assert
        operation = (LdbcQuery2) reader.next();
        assertThat(operation.getPersonIdQ2(), is(12094628092905L));
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 28);
        assertThat(operation.getMaxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.getPersonIdQ2(), is(9895606011404L));
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 28);
        assertThat(operation.getMaxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.getPersonIdQ2(), is(14293651244033L));
        calendar.clear();
        calendar.set(2013, Calendar.FEBRUARY, 2);
        assertThat(operation.getMaxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery2) reader.next();
        assertThat(operation.getPersonIdQ2(), is(13194139602632L));
        calendar.clear();
        calendar.set(2013, Calendar.OCTOBER, 16);
        assertThat(operation.getMaxDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery3Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1)).thenReturn(9895605643992l).thenReturn(979201l).thenReturn(129891l).thenReturn(13194140498760l);
        when(rs.getString(4))
            .thenReturn("2011-12-01")
            .thenReturn("2012-04-01" )
            .thenReturn("2011-05-01" )
            .thenReturn("2010-12-01" );
        when(rs.getInt(5)).thenReturn(53).thenReturn(64).thenReturn(58).thenReturn(53);
        when(rs.getString(2)).thenReturn("Taiwan").thenReturn("Nicaragua").thenReturn("Colombia").thenReturn("Lithuania");
        when(rs.getString(3)).thenReturn("Bulgaria").thenReturn("Afghanistan").thenReturn("Lithuania").thenReturn("Afghanistan");
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query3Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );
        LdbcQuery3 operation;
        // Assert
        operation = (LdbcQuery3) reader.next();
        assertThat(operation.getPersonIdQ3(), is(9895605643992L));
        assertThat(operation.getCountryXName(), is("Taiwan"));
        assertThat(operation.getCountryYName(), is("Bulgaria"));
        assertThat(operation.getDurationDays(), is(53));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 1);
        assertThat(operation.getStartDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.getPersonIdQ3(), is(979201L));
        assertThat(operation.getCountryXName(), is("Nicaragua"));
        assertThat(operation.getCountryYName(), is("Afghanistan"));
        assertThat(operation.getDurationDays(), is(64));
        calendar.clear();
        calendar.set(2012, Calendar.APRIL, 1);
        assertThat(operation.getStartDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.getPersonIdQ3(), is(129891L));
        assertThat(operation.getCountryXName(), is("Colombia"));
        assertThat(operation.getCountryYName(), is("Lithuania"));
        assertThat(operation.getDurationDays(), is(58));
        calendar.clear();
        calendar.set(2011, Calendar.MAY, 1);

        operation = (LdbcQuery3) reader.next();
        assertThat(operation.getPersonIdQ3(), is(13194140498760L));
        assertThat(operation.getCountryXName(), is("Lithuania"));
        assertThat(operation.getCountryYName(), is("Afghanistan"));
        assertThat(operation.getDurationDays(), is(53));
        calendar.clear();
        calendar.set(2010, Calendar.DECEMBER, 1);
        assertThat(operation.getStartDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery4Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(12094628092905l)
            .thenReturn(9895606011404l)
            .thenReturn(14293651244033l)
            .thenReturn(13194139602632l);
        when(rs.getString(2))
            .thenReturn("2011-04-01" )
            .thenReturn("2012-01-01" )
            .thenReturn("2011-07-01" )
            .thenReturn("2011-07-01" );
        when(rs.getInt(3))
            .thenReturn(43)
            .thenReturn(36)
            .thenReturn(57)
            .thenReturn(81);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query4Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );
        LdbcQuery4 operation;
        // Assert
        operation = (LdbcQuery4) reader.next();
        assertThat(operation.getPersonIdQ4(), is(12094628092905L));
        assertThat(operation.getDurationDays(), is(43));
        calendar.clear();
        calendar.set(2011, Calendar.APRIL, 1);
        assertThat(operation.getStartDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.getPersonIdQ4(), is(9895606011404L));
        assertThat(operation.getDurationDays(), is(36));
        calendar.clear();
        calendar.set(2012, Calendar.JANUARY, 1);
        assertThat(operation.getStartDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.getPersonIdQ4(), is(14293651244033L));
        assertThat(operation.getDurationDays(), is(57));
        calendar.clear();
        calendar.set(2011, Calendar.JULY, 1);
        assertThat(operation.getStartDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery4) reader.next();
        assertThat(operation.getPersonIdQ4(), is(13194139602632L));
        assertThat(operation.getDurationDays(), is(81));
        calendar.clear();
        calendar.set(2011, Calendar.JULY, 1);
        assertThat(operation.getStartDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery5Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(9895605643992l)
            .thenReturn(979201l)
            .thenReturn(129891l)
            .thenReturn(13194140498760l);
        when(rs.getString(2))
            .thenReturn("2012-12-15")
            .thenReturn("2012-12-16")
            .thenReturn("2012-12-14")
            .thenReturn("2012-12-12");

        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query5Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery5 operation;

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.getPersonIdQ5(), is(9895605643992L));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 15);
        assertThat(operation.getMinDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.getPersonIdQ5(), is(979201L));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 16);
        assertThat(operation.getMinDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.getPersonIdQ5(), is(129891L));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 14);
        assertThat(operation.getMinDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery5) reader.next();
        assertThat(operation.getPersonIdQ5(), is(13194140498760L));
        calendar.clear();
        calendar.set(2012, Calendar.DECEMBER, 12);
        assertThat(operation.getMinDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery6Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getString(2))
            .thenReturn("Jiang_Zemin")
            .thenReturn("Nino_Rota")
            .thenReturn("John_VI_of_Portugal")
            .thenReturn("Nikolai_Gogol");
        when(rs.getLong(1))
            .thenReturn(9895605643992l)
            .thenReturn(979201l)
            .thenReturn(129891l)
            .thenReturn(13194140498760l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query6Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery6 operation;

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.getPersonIdQ6(), is(9895605643992L));
        assertThat(operation.getTagName(), is("Jiang_Zemin"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.getPersonIdQ6(), is(979201L));
        assertThat(operation.getTagName(), is("Nino_Rota"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.getPersonIdQ6(), is(129891L));
        assertThat(operation.getTagName(), is("John_VI_of_Portugal"));

        operation = (LdbcQuery6) reader.next();
        assertThat(operation.getPersonIdQ6(), is(13194140498760L));
        assertThat(operation.getTagName(), is("Nikolai_Gogol"));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery7Events() throws WorkloadException, SQLException{
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(16492675436774l)
            .thenReturn(14293651330072l)
            .thenReturn(4398047140913l)
            .thenReturn(13194140823804l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query7Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery7 operation;

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.getPersonIdQ7(), is(16492675436774L));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.getPersonIdQ7(), is(14293651330072L));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.getPersonIdQ7(), is(4398047140913L));

        operation = (LdbcQuery7) reader.next();
        assertThat(operation.getPersonIdQ7(), is(13194140823804L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery8Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(15393164184077l)
            .thenReturn(15393163594341l)
            .thenReturn(7696582593995l)
            .thenReturn(15393162809578l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query8Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );
        // Assert
        LdbcQuery8 operation;

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.getPersonIdQ8(), is(15393164184077L));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.getPersonIdQ8(), is(15393163594341L));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.getPersonIdQ8(), is(7696582593995L));

        operation = (LdbcQuery8) reader.next();
        assertThat(operation.getPersonIdQ8(), is(15393162809578L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery9Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(9895605643992l)
            .thenReturn(979201l)
            .thenReturn(129891l)
            .thenReturn(13194140498760l);
        when(rs.getString(2))
            .thenReturn("2011-12-22" )
            .thenReturn("2011-11-19" )
            .thenReturn("2011-11-20" )
            .thenReturn("2011-12-01" );
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query9Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );
        // Assert
        LdbcQuery9 operation;

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.getPersonIdQ9(), is(9895605643992L));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 22);
        assertThat(operation.getMaxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.getPersonIdQ9(), is(979201L));
        calendar.clear();
        calendar.set(2011, Calendar.NOVEMBER, 19);
        assertThat(operation.getMaxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.getPersonIdQ9(), is(129891L));
        calendar.clear();
        calendar.set(2011, Calendar.NOVEMBER, 20);
        assertThat(operation.getMaxDate().getTime(), is(calendar.getTime().getTime()));

        operation = (LdbcQuery9) reader.next();
        assertThat(operation.getPersonIdQ9(), is(13194140498760L));
        calendar.clear();
        calendar.set(2011, Calendar.DECEMBER, 1);
        assertThat(operation.getMaxDate().getTime(), is(calendar.getTime().getTime()));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery10Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getInt(2))
            .thenReturn(2)
            .thenReturn(4)
            .thenReturn(2)
            .thenReturn(3);
        when(rs.getLong(1))
            .thenReturn(9895605643992l)
            .thenReturn(979201l)
            .thenReturn(129891l)
            .thenReturn(13194140498760l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query10Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery10 operation;

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.getPersonIdQ10(), is(9895605643992L));
        assertThat(operation.getMonth(), is(2));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.getPersonIdQ10(), is(979201L));
        assertThat(operation.getMonth(), is(4));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.getPersonIdQ10(), is(129891L));
        assertThat(operation.getMonth(), is(2));

        operation = (LdbcQuery10) reader.next();
        assertThat(operation.getPersonIdQ10(), is(13194140498760L));
        assertThat(operation.getMonth(), is(3));


        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery11Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getInt(3))
            .thenReturn(2013)
            .thenReturn(1998)
            .thenReturn(1974)
            .thenReturn(1984);
        when(rs.getString(2))
            .thenReturn("Taiwan")
            .thenReturn("Nicaragua")
            .thenReturn("Colombia")
            .thenReturn("Lithuania");
        when(rs.getLong(1))
            .thenReturn(9895605643992l)
            .thenReturn(979201l)
            .thenReturn(129891l)
            .thenReturn(13194140498760l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query11Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery11 operation;

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.getPersonIdQ11(), is(9895605643992L));
        assertThat(operation.getCountryName(), is("Taiwan"));
        assertThat(operation.getWorkFromYear(), is(2013));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.getPersonIdQ11(), is(979201L));
        assertThat(operation.getCountryName(), is("Nicaragua"));
        assertThat(operation.getWorkFromYear(), is(1998));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.getPersonIdQ11(), is(129891L));
        assertThat(operation.getCountryName(), is("Colombia"));
        assertThat(operation.getWorkFromYear(), is(1974));

        operation = (LdbcQuery11) reader.next();
        assertThat(operation.getPersonIdQ11(), is(13194140498760L));
        assertThat(operation.getCountryName(), is("Lithuania"));
        assertThat(operation.getWorkFromYear(), is(1984));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery12Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getString(2))
            .thenReturn("SoccerManager")
            .thenReturn("Chancellor")
            .thenReturn("EurovisionSongContestEntry")
            .thenReturn("GolfPlayer");
        when(rs.getLong(1))
            .thenReturn(12094628092905l)
            .thenReturn(9895606011404l)
            .thenReturn(14293651244033l)
            .thenReturn(13194139602632l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query12Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery12 operation;

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.getPersonIdQ12(), is(12094628092905L));
        assertThat(operation.getTagClassName(), equalTo("SoccerManager"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.getPersonIdQ12(), is(9895606011404L));
        assertThat(operation.getTagClassName(), equalTo("Chancellor"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.getPersonIdQ12(), is(14293651244033L));
        assertThat(operation.getTagClassName(), equalTo("EurovisionSongContestEntry"));

        operation = (LdbcQuery12) reader.next();
        assertThat(operation.getPersonIdQ12(), is(13194139602632L));
        assertThat(operation.getTagClassName(), equalTo("GolfPlayer"));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery13Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(9895605643992l)
            .thenReturn(979201l)
            .thenReturn(129891l)
            .thenReturn(13194140498760l);
        when(rs.getLong(2))
            .thenReturn(1099512323797l)
            .thenReturn(95384l)
            .thenReturn(9895606000517l)
            .thenReturn(7696582276748l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query13Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery13 operation;

        operation = (LdbcQuery13) reader.next();
        assertThat(operation.getPerson1IdQ13StartNode(), is(9895605643992L));
        assertThat(operation.getPerson2IdQ13EndNode(), is(1099512323797L));

        operation = (LdbcQuery13) reader.next();
        assertThat(operation.getPerson1IdQ13StartNode(), is(979201L));
        assertThat(operation.getPerson2IdQ13EndNode(), is(95384L));

        operation = (LdbcQuery13) reader.next();
        assertThat(operation.getPerson1IdQ13StartNode(), is(129891L));
        assertThat(operation.getPerson2IdQ13EndNode(), is(9895606000517L));

        operation = (LdbcQuery13) reader.next();
        assertThat(operation.getPerson1IdQ13StartNode(), is(13194140498760L));
        assertThat(operation.getPerson2IdQ13EndNode(), is(7696582276748L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery14Events() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(9895605643992l)
            .thenReturn(979201l)
            .thenReturn(129891l)
            .thenReturn(13194140498760l);
        when(rs.getLong(2))
            .thenReturn(4398046737628l)
            .thenReturn(1277748l)
            .thenReturn(6597069967720l)
            .thenReturn(3298534975254l);
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query14Decoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery14 operation;

        operation = (LdbcQuery14) reader.next();
        assertThat(operation.getPerson1IdQ14StartNode(), is(9895605643992L));
        assertThat(operation.getPerson2IdQ14EndNode(), is(4398046737628L));

        operation = (LdbcQuery14) reader.next();
        assertThat(operation.getPerson1IdQ14StartNode(), is(979201L));
        assertThat(operation.getPerson2IdQ14EndNode(), is(1277748L));

        operation = (LdbcQuery14) reader.next();
        assertThat(operation.getPerson1IdQ14StartNode(), is(129891L));
        assertThat(operation.getPerson2IdQ14EndNode(), is(6597069967720L));

        operation = (LdbcQuery14) reader.next();
        assertThat(operation.getPerson1IdQ14StartNode(), is(13194140498760L));
        assertThat(operation.getPerson2IdQ14EndNode(), is(3298534975254L));

        assertThat(reader.hasNext(), is(false));
    }

}

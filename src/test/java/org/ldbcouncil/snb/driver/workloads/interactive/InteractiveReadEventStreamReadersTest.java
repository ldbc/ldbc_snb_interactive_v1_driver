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
import java.sql.Timestamp;
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
        when(rs.getLong(1))
            .thenReturn(10995117334833l)
            .thenReturn(14293651244033l)
            .thenReturn(6597070008725l)
            .thenReturn(2199023331001l);
        when(rs.getString(2))
            .thenReturn("John")
            .thenReturn("Yang")
            .thenReturn("A.")
            .thenReturn("Chen");
        when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.getPersonIdQ1(), is(14293651244033L));
        assertThat(operation.getFirstName(), equalTo("Yang"));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.getPersonIdQ1(), is(6597070008725L));
        assertThat(operation.getFirstName(), equalTo("A."));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));

        operation = (LdbcQuery1) reader.next();
        assertThat(operation.getPersonIdQ1(), is(2199023331001L));
        assertThat(operation.getFirstName(), equalTo("Chen"));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));

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
        when(rs.getTimestamp(2))
            .thenReturn(Timestamp.valueOf("2013-01-28 01:00:00.000"))
            .thenReturn(Timestamp.valueOf("2013-01-28 01:00:00.000"))
            .thenReturn(Timestamp.valueOf("2013-02-02 01:00:00.000"))
            .thenReturn(Timestamp.valueOf("2013-10-16 02:00:00.000"));
        when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery2) reader.next();
        assertThat(operation.getPersonIdQ2(), is(9895606011404L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery2) reader.next();
        assertThat(operation.getPersonIdQ2(), is(14293651244033L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery2) reader.next();
        assertThat(operation.getPersonIdQ2(), is(13194139602632L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery3aEvents() throws WorkloadException, SQLException {
        // Arrange
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1)).thenReturn(9895605643992l).thenReturn(979201l).thenReturn(129891l).thenReturn(13194140498760l);
        when(rs.getString(2)).thenReturn("Taiwan").thenReturn("Nicaragua").thenReturn("Colombia").thenReturn("Lithuania");
        when(rs.getString(3)).thenReturn("Bulgaria").thenReturn("Afghanistan").thenReturn("Lithuania").thenReturn("Afghanistan");
        when(rs.getTimestamp(4))
            .thenReturn(Timestamp.valueOf("2011-12-01 01:00:00.000"))
            .thenReturn(Timestamp.valueOf("2012-04-01 02:00:00.000" ))
            .thenReturn(Timestamp.valueOf("2011-05-01 02:00:00.000" ))
            .thenReturn(Timestamp.valueOf("2010-12-01 01:00:00.000" ));
        when(rs.getInt(5)).thenReturn(53).thenReturn(64).thenReturn(58).thenReturn(53);

        when(rs.getString(6))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(7))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query3aDecoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );
        LdbcQuery3a operation;
        // Assert
        operation = (LdbcQuery3a) reader.next();
        assertThat(operation.getPersonIdQ3(), is(9895605643992L));
        assertThat(operation.getCountryXName(), is("Taiwan"));
        assertThat(operation.getCountryYName(), is("Bulgaria"));
        assertThat(operation.getDurationDays(), is(53));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery3a) reader.next();
        assertThat(operation.getPersonIdQ3(), is(979201L));
        assertThat(operation.getCountryXName(), is("Nicaragua"));
        assertThat(operation.getCountryYName(), is("Afghanistan"));
        assertThat(operation.getDurationDays(), is(64));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery3a) reader.next();
        assertThat(operation.getPersonIdQ3(), is(129891L));
        assertThat(operation.getCountryXName(), is("Colombia"));
        assertThat(operation.getCountryYName(), is("Lithuania"));
        assertThat(operation.getDurationDays(), is(58));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery3a) reader.next();
        assertThat(operation.getPersonIdQ3(), is(13194140498760L));
        assertThat(operation.getCountryXName(), is("Lithuania"));
        assertThat(operation.getCountryYName(), is("Afghanistan"));
        assertThat(operation.getDurationDays(), is(53));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
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
        when(rs.getTimestamp(2))
            .thenReturn(Timestamp.valueOf("2011-04-01 02:00:00.000" ))
            .thenReturn(Timestamp.valueOf("2012-01-01 01:00:00.000" ))
            .thenReturn(Timestamp.valueOf("2011-07-01 02:00:00.000" ))
            .thenReturn(Timestamp.valueOf("2011-07-01 02:00:00.000" ));
        when(rs.getInt(3))
            .thenReturn(43)
            .thenReturn(36)
            .thenReturn(57)
            .thenReturn(81);
        when(rs.getString(4))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(5))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery4) reader.next();
        assertThat(operation.getPersonIdQ4(), is(9895606011404L));
        assertThat(operation.getDurationDays(), is(36));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery4) reader.next();
        assertThat(operation.getPersonIdQ4(), is(14293651244033L));
        assertThat(operation.getDurationDays(), is(57));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery4) reader.next();
        assertThat(operation.getPersonIdQ4(), is(13194139602632L));
        assertThat(operation.getDurationDays(), is(81));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
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
        when(rs.getTimestamp(2))
            .thenReturn(Timestamp.valueOf("2012-12-15 01:00:00.000"))
            .thenReturn(Timestamp.valueOf("2012-12-16 01:00:00.000"))
            .thenReturn(Timestamp.valueOf("2012-12-14 01:00:00.000"))
            .thenReturn(Timestamp.valueOf("2012-12-12 01:00:00.000"));
        when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery5) reader.next();
        assertThat(operation.getPersonIdQ5(), is(979201L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery5) reader.next();
        assertThat(operation.getPersonIdQ5(), is(129891L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery5) reader.next();
        assertThat(operation.getPersonIdQ5(), is(13194140498760L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
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
        when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery6) reader.next();
        assertThat(operation.getPersonIdQ6(), is(979201L));
        assertThat(operation.getTagName(), is("Nino_Rota"));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery6) reader.next();
        assertThat(operation.getPersonIdQ6(), is(129891L));
        assertThat(operation.getTagName(), is("John_VI_of_Portugal"));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery6) reader.next();
        assertThat(operation.getPersonIdQ6(), is(13194140498760L));
        assertThat(operation.getTagName(), is("Nikolai_Gogol"));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
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
            when(rs.getString(2))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(3))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery7) reader.next();
        assertThat(operation.getPersonIdQ7(), is(14293651330072L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery7) reader.next();
        assertThat(operation.getPersonIdQ7(), is(4398047140913L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery7) reader.next();
        assertThat(operation.getPersonIdQ7(), is(13194140823804L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
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
            when(rs.getString(2))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(3))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery8) reader.next();
        assertThat(operation.getPersonIdQ8(), is(15393163594341L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery8) reader.next();
        assertThat(operation.getPersonIdQ8(), is(7696582593995L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery8) reader.next();
        assertThat(operation.getPersonIdQ8(), is(15393162809578L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
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
        when(rs.getTimestamp(2))
            .thenReturn(Timestamp.valueOf("2011-12-22 01:00:00.000" ))
            .thenReturn(Timestamp.valueOf("2011-11-19 01:00:00.000" ))
            .thenReturn(Timestamp.valueOf("2011-11-20 01:00:00.000" ))
            .thenReturn(Timestamp.valueOf("2011-12-01 01:00:00.000" ));
            when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery9) reader.next();
        assertThat(operation.getPersonIdQ9(), is(979201L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery9) reader.next();
        assertThat(operation.getPersonIdQ9(), is(129891L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery9) reader.next();
        assertThat(operation.getPersonIdQ9(), is(13194140498760L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
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
            when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery10) reader.next();
        assertThat(operation.getPersonIdQ10(), is(979201L));
        assertThat(operation.getMonth(), is(4));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery10) reader.next();
        assertThat(operation.getPersonIdQ10(), is(129891L));
        assertThat(operation.getMonth(), is(2));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery10) reader.next();
        assertThat(operation.getPersonIdQ10(), is(13194140498760L));
        assertThat(operation.getMonth(), is(3));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));

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
            when(rs.getString(4))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(5))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery11) reader.next();
        assertThat(operation.getPersonIdQ11(), is(979201L));
        assertThat(operation.getCountryName(), is("Nicaragua"));
        assertThat(operation.getWorkFromYear(), is(1998));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery11) reader.next();
        assertThat(operation.getPersonIdQ11(), is(129891L));
        assertThat(operation.getCountryName(), is("Colombia"));
        assertThat(operation.getWorkFromYear(), is(1974));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery11) reader.next();
        assertThat(operation.getPersonIdQ11(), is(13194140498760L));
        assertThat(operation.getCountryName(), is("Lithuania"));
        assertThat(operation.getWorkFromYear(), is(1984));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
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
            when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
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
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery12) reader.next();
        assertThat(operation.getPersonIdQ12(), is(9895606011404L));
        assertThat(operation.getTagClassName(), equalTo("Chancellor"));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery12) reader.next();
        assertThat(operation.getPersonIdQ12(), is(14293651244033L));
        assertThat(operation.getTagClassName(), equalTo("EurovisionSongContestEntry"));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery12) reader.next();
        assertThat(operation.getPersonIdQ12(), is(13194139602632L));
        assertThat(operation.getTagClassName(), equalTo("GolfPlayer"));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery13bEvents() throws WorkloadException, SQLException {
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
            when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query13bDecoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);
        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery13b operation;

        operation = (LdbcQuery13b) reader.next();
        assertThat(operation.getPerson1IdQ13StartNode(), is(9895605643992L));
        assertThat(operation.getPerson2IdQ13EndNode(), is(1099512323797L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery13b) reader.next();
        assertThat(operation.getPerson1IdQ13StartNode(), is(979201L));
        assertThat(operation.getPerson2IdQ13EndNode(), is(95384L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery13b) reader.next();
        assertThat(operation.getPerson1IdQ13StartNode(), is(129891L));
        assertThat(operation.getPerson2IdQ13EndNode(), is(9895606000517L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery13b) reader.next();
        assertThat(operation.getPerson1IdQ13StartNode(), is(13194140498760L));
        assertThat(operation.getPerson2IdQ13EndNode(), is(7696582276748L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllQuery14bEvents() throws WorkloadException, SQLException {
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
        when(rs.getString(3))
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735")
            .thenReturn("2012-07-29 08:52:02.735");
        when(rs.getString(4))
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0")
            .thenReturn("2019-12-30 00:00:00.0");
        EventStreamReader.EventDecoder<Operation> decoder = new QueryEventStreamReader.Query14bDecoder();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new QueryEventStreamReader(
            opStream
        );

        // Assert
        LdbcQuery14b operation;

        operation = (LdbcQuery14b) reader.next();
        assertThat(operation.getPerson1IdQ14StartNode(), is(9895605643992L));
        assertThat(operation.getPerson2IdQ14EndNode(), is(4398046737628L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery14b) reader.next();
        assertThat(operation.getPerson1IdQ14StartNode(), is(979201L));
        assertThat(operation.getPerson2IdQ14EndNode(), is(1277748L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery14b) reader.next();
        assertThat(operation.getPerson1IdQ14StartNode(), is(129891L));
        assertThat(operation.getPerson2IdQ14EndNode(), is(6597069967720L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        operation = (LdbcQuery14b) reader.next();
        assertThat(operation.getPerson1IdQ14StartNode(), is(13194140498760L));
        assertThat(operation.getPerson2IdQ14EndNode(), is(3298534975254L));
        assertThat(operation.dependencyTimeStamp(), equalTo(1343551922735L));
        assertThat(operation.expiryTimeStamp(), equalTo(1577664000000L));
        assertThat(reader.hasNext(), is(false));
    }
}

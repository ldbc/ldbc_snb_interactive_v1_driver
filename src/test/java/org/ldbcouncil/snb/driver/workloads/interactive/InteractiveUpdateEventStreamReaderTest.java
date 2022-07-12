package org.ldbcouncil.snb.driver.workloads.interactive;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.csv.ParquetLoader;
import org.ldbcouncil.snb.driver.csv.DuckDbConnectionState;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InteractiveUpdateEventStreamReaderTest
{
    private DuckDbConnectionState db;
    private Statement stmt;

    /**
     * Initialize mock objects used in all the tests
     * @throws SQLException
     */
    @Before
    public void init() throws SQLException {
        Connection connection = mock(Connection.class);
        db = mock(DuckDbConnectionState.class);
        when(db.getConnection()).thenReturn(connection);
        stmt = mock(Statement.class);
        when(connection.createStatement()).thenReturn(stmt);
    }

    // TODO: Complete LdbcInsert1AddPerson test
    @Ignore
    public void shouldParseAllInsert1Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2)) // PersonId
            .thenReturn(2336462220352l)
            .thenReturn(2336462274764l)
            .thenReturn(2336462234123l)
            .thenReturn(2336462255683l);
        when(rs.getString(3)) // firstName
            .thenReturn("William")
            .thenReturn("Jean-Luc")
            .thenReturn("Wesley")
            .thenReturn("Miles");
        when(rs.getString(4)) // lastName
            .thenReturn("Riker")
            .thenReturn("Picard")
            .thenReturn("Crusher")
            .thenReturn("O'Brien");
        when(rs.getString(5)) // gender
            .thenReturn("male")
            .thenReturn("male")
            .thenReturn("male")
            .thenReturn("male");
        //Birthday
        when(rs.getString(7))
            .thenReturn("127.0.0.1")
            .thenReturn("127.0.0.2")
            .thenReturn("127.0.0.3")
            .thenReturn("127.0.0.4");
        when(rs.getString(8)) //BrowserUsed
            .thenReturn("Netscape")
            .thenReturn("Mosaic")
            .thenReturn("Internet Explorer")
            .thenReturn("Vivaldi");
        when(rs.getLong(9)) // cityId
            .thenReturn(100l)
            .thenReturn(200l)
            .thenReturn(300l)
            .thenReturn(400l);
        when(rs.getString(10))
            .thenReturn("EN;HU")
            .thenReturn("EN;ES;NL")
            .thenReturn("DE;EN;PL")
            .thenReturn("EN");
        when(rs.getString(11))
            .thenReturn("w.riker@starfleet.com;willyriker@aol.com")
            .thenReturn("j.picard@starfleet.com;jean.luc.picard@gmx.com")
            .thenReturn("")
            .thenReturn("m.obrien@starfleet.com");
        when(rs.getString(12))
            .thenReturn("2678;110")
            .thenReturn("1925")
            .thenReturn("12;6842;7895")
            .thenReturn("");
        when(rs.getString(13))
            .thenReturn("100,2459;200,2012")
            .thenReturn("100,2459")
            .thenReturn("100,2459")
            .thenReturn("");
        when(rs.getString(14))
            .thenReturn("100,8766;200,5432;300,2876")
            .thenReturn("100,4587;200,5643")
            .thenReturn("")
            .thenReturn("100,4587");
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderAddPerson();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcInsert1AddPerson operation;
        operation = (LdbcInsert1AddPerson) reader.next();
        assertThat(operation.getPersonId(), is(2336462220352l));
        assertThat(operation.getPersonFirstName(), is("William"));
        assertThat(operation.getPersonLastName(), is("Riker"));
        assertThat(operation.getLocationIp(), is("127.0.0.1"));
        assertThat(operation.getBrowserUsed(), is("Netscape"));
        assertThat(operation.getCityId(), is(100l));

        assertThat(operation.getCreationDate().getTime(), is(1354158403640L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcInsert1AddPerson) reader.next();
        assertThat(operation.getPersonId(), is(2336462274764l));
        assertThat(operation.getPersonFirstName(), is("Jean-Luc"));
        assertThat(operation.getPersonLastName(), is("Picard"));
        assertThat(operation.getLocationIp(), is("127.0.0.2"));
        assertThat(operation.getBrowserUsed(), is("Mosaic"));
        assertThat(operation.getCityId(), is(200l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert1AddPerson) reader.next();
        assertThat(operation.getPersonId(), is(2336462234123l));
        assertThat(operation.getPersonFirstName(), is("Wesley"));
        assertThat(operation.getPersonLastName(), is("Crusher"));
        assertThat(operation.getLocationIp(), is("127.0.0.3"));
        assertThat(operation.getBrowserUsed(), is("Internet Explorer"));
        assertThat(operation.getCityId(), is(300l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert1AddPerson) reader.next();
        assertThat(operation.getPersonId(), is(2336462255683l));
        assertThat(operation.getPersonFirstName(), is("Miles"));
        assertThat(operation.getPersonLastName(), is("O'Brien"));
        assertThat(operation.getLocationIp(), is("127.0.0.4"));
        assertThat(operation.getBrowserUsed(), is("Vivaldi"));
        assertThat(operation.getCityId(), is(400l));
        assertThat(operation.getCreationDate().getTime(), is(1354164952506L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllInsert2Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2))
            .thenReturn(15393162789345l)
            .thenReturn(15393162794683l)
            .thenReturn(8796093026612l)
            .thenReturn(26388279069478l);
        when(rs.getLong(3))
            .thenReturn(2336468703973l)
            .thenReturn(2336467057324l)
            .thenReturn(2336462285980l)
            .thenReturn(2336465315493l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderAddLikePost();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcInsert2AddPostLike operation;

        operation = (LdbcInsert2AddPostLike) reader.next();
        assertThat(operation.getPersonId(), is(15393162789345l));
        assertThat(operation.getPostId(), is(2336468703973l));
        assertThat(operation.getCreationDate().getTime(), is(1354158403640L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcInsert2AddPostLike) reader.next();
        assertThat(operation.getPersonId(), is(15393162794683l));
        assertThat(operation.getPostId(), is(2336467057324l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert2AddPostLike) reader.next();
        assertThat(operation.getPersonId(), is(8796093026612l));
        assertThat(operation.getPostId(), is(2336462285980l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert2AddPostLike) reader.next();
        assertThat(operation.getPersonId(), is(26388279069478l));
        assertThat(operation.getPostId(), is(2336465315493l));
        assertThat(operation.getCreationDate().getTime(), is(1354164952506L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllInsert3Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2))
            .thenReturn(15393162789345l)
            .thenReturn(15393162794683l)
            .thenReturn(8796093026612l)
            .thenReturn(26388279069478l);
        when(rs.getLong(3))
            .thenReturn(2336468703973l)
            .thenReturn(2336467057324l)
            .thenReturn(2336462285980l)
            .thenReturn(2336465315493l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderAddLikeComment();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcInsert3AddCommentLike operation;

        operation = (LdbcInsert3AddCommentLike) reader.next();
        assertThat(operation.getPersonId(), is(15393162789345l));
        assertThat(operation.getCommentId(), is(2336468703973l));
        assertThat(operation.getCreationDate().getTime(), is(1354158403640L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcInsert3AddCommentLike) reader.next();
        assertThat(operation.getPersonId(), is(15393162794683l));
        assertThat(operation.getCommentId(), is(2336467057324l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert3AddCommentLike) reader.next();
        assertThat(operation.getPersonId(), is(8796093026612l));
        assertThat(operation.getCommentId(), is(2336462285980l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert3AddCommentLike) reader.next();
        assertThat(operation.getPersonId(), is(26388279069478l));
        assertThat(operation.getCommentId(), is(2336465315493l));
        assertThat(operation.getCreationDate().getTime(), is(1354164952506L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllInsert4Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354157772178L)
            .thenReturn(1354158101157L)
            .thenReturn(1354159101925L)
            .thenReturn(1354159537443L);
        when(rs.getLong(2))
            .thenReturn(2336462220352l)
            .thenReturn(2336462274764l)
            .thenReturn(2336462234123l)
            .thenReturn(2336462255683l);
        when(rs.getString(3))
            .thenReturn("Album 31 of Anand Rao")
            .thenReturn("Album 0 of A. Rao")
            .thenReturn("Album 6 of Alfonso Chavez")
            .thenReturn("Album 14 of Ivan Ivanov");
        when(rs.getLong(4))
            .thenReturn(2199023256479l)
            .thenReturn(15393162797039l)
            .thenReturn(4398046516485l)
            .thenReturn(8796093032781l);
        when(rs.getString(5))
            .thenReturn("9067")
            .thenReturn("7509")
            .thenReturn("2999;125")
            .thenReturn("1;2;3");
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderAddForum();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcInsert4AddForum operation;

        operation = (LdbcInsert4AddForum) reader.next();
        assertThat(operation.getForumId(), is(2336462220352l));
        assertThat(operation.getForumTitle(), is("Album 31 of Anand Rao"));
        assertThat(operation.getModeratorPersonId(), is(2199023256479l));
        List<Long> tagIds1 = new ArrayList<Long>(){
            {
                add(9067l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds1));
        assertThat(operation.getCreationDate().getTime(), is(1354157772178L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354157772178L));

        operation = (LdbcInsert4AddForum) reader.next();
        assertThat(operation.getForumId(), is(2336462274764l));
        assertThat(operation.getForumTitle(), is("Album 0 of A. Rao"));
        assertThat(operation.getModeratorPersonId(), is(15393162797039l));
        List<Long> tagIds2 = new ArrayList<Long>(){
            {
                add(7509l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds2));
        assertThat(operation.getCreationDate().getTime(), is(1354158101157L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158101157L));

        operation = (LdbcInsert4AddForum) reader.next();
        assertThat(operation.getForumId(), is(2336462234123l));
        assertThat(operation.getForumTitle(), is("Album 6 of Alfonso Chavez"));
        assertThat(operation.getModeratorPersonId(), is(4398046516485l));
        List<Long> tagIds3 = new ArrayList<Long>(){
            {
                add(2999l); add(125l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds3));
        assertThat(operation.getCreationDate().getTime(), is(1354159101925L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159101925L));

        operation = (LdbcInsert4AddForum) reader.next();
        assertThat(operation.getForumId(), is(2336462255683l));
        assertThat(operation.getForumTitle(), is("Album 14 of Ivan Ivanov"));
        assertThat(operation.getModeratorPersonId(), is(8796093032781l));
        List<Long> tagIds4 = new ArrayList<Long>(){
            {
                add(1l); add(2l); add(3l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds4));
        assertThat(operation.getCreationDate().getTime(), is(1354159537443L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159537443L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllInsert5Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2))
            .thenReturn(2336468703973l)
            .thenReturn(2336467057324l)
            .thenReturn(2336462285980l)
            .thenReturn(2336465315493l);
        when(rs.getLong(3))
            .thenReturn(15393162789345l)
            .thenReturn(15393162794683l)
            .thenReturn(8796093026612l)
            .thenReturn(26388279069478l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderAddForumMembership();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcInsert5AddForumMembership operation;

        operation = (LdbcInsert5AddForumMembership) reader.next();
        assertThat(operation.getPersonId(), is(15393162789345l));
        assertThat(operation.getForumId(), is(2336468703973l));
        assertThat(operation.getCreationDate().getTime(), is(1354158403640L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcInsert5AddForumMembership) reader.next();
        assertThat(operation.getPersonId(), is(15393162794683l));
        assertThat(operation.getForumId(), is(2336467057324l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert5AddForumMembership) reader.next();
        assertThat(operation.getPersonId(), is(8796093026612l));
        assertThat(operation.getForumId(), is(2336462285980l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert5AddForumMembership) reader.next();
        assertThat(operation.getPersonId(), is(26388279069478l));
        assertThat(operation.getForumId(), is(2336465315493l));
        assertThat(operation.getCreationDate().getTime(), is(1354164952506L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllInsert6Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354157772178L)
            .thenReturn(1354158101157L)
            .thenReturn(1354159101925L)
            .thenReturn(1354159537443L);
        when(rs.getLong(2)) // PostId
            .thenReturn(2336462220352l)
            .thenReturn(2336462274764l)
            .thenReturn(2336462234123l)
            .thenReturn(2336462255683l);
        when(rs.getString(3))
            .thenReturn("image1")
            .thenReturn("")
            .thenReturn("image3")
            .thenReturn("");
        when(rs.getString(4))
            .thenReturn("127.0.0.1")
            .thenReturn("127.0.0.2")
            .thenReturn("127.0.0.3")
            .thenReturn("127.0.0.4");
        when(rs.getString(5))
            .thenReturn("Netscape")
            .thenReturn("Mosaic")
            .thenReturn("Internet Explorer")
            .thenReturn("Vivaldi");
        when(rs.getString(6))
            .thenReturn("NL")
            .thenReturn("EN")
            .thenReturn("ES")
            .thenReturn("DE");
        when(rs.getString(7))
            .thenReturn("content1")
            .thenReturn("content2")
            .thenReturn("content3")
            .thenReturn("content4");
        when(rs.getInt(8))
            .thenReturn(1)
            .thenReturn(2)
            .thenReturn(3)
            .thenReturn(4);
        when(rs.getLong(9))
            .thenReturn(2199023256479l)
            .thenReturn(15393162797039l)
            .thenReturn(4398046516485l)
            .thenReturn(8796093032781l);
        when(rs.getLong(10))
            .thenReturn(3199023256479l)
            .thenReturn(35393162797039l)
            .thenReturn(3398046516485l)
            .thenReturn(3796093032781l);
        when(rs.getLong(11))
            .thenReturn(4199023256479l)
            .thenReturn(45393162797039l)
            .thenReturn(4398046516485l)
            .thenReturn(4796093032781l);
        when(rs.getString(12))
            .thenReturn("9067")
            .thenReturn("7509")
            .thenReturn("2999;125")
            .thenReturn("1;2;3");

        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderAddPost();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcInsert6AddPost operation;

        operation = (LdbcInsert6AddPost) reader.next();
        assertThat(operation.getPostId(), is(2336462220352l));
        assertThat(operation.getLocationIp(), is("127.0.0.1"));
        assertThat(operation.getImageFile(), is("image1"));
        assertThat(operation.getBrowserUsed(), is("Netscape"));
        assertThat(operation.getContent(), is("content1"));
        assertThat(operation.getLanguage(), is("NL"));
        assertThat(operation.getLength(), is(1));
        assertThat(operation.getAuthorPersonId(), is(2199023256479l));
        assertThat(operation.getForumId(), is(3199023256479l));
        assertThat(operation.getCountryId(), is(4199023256479l));
        List<Long> tagIds1 = new ArrayList<Long>(){
            {
                add(9067l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds1));
        assertThat(operation.getCreationDate().getTime(), is(1354157772178L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354157772178L));

        operation = (LdbcInsert6AddPost) reader.next();
        assertThat(operation.getPostId(), is(2336462274764l));
        assertThat(operation.getLocationIp(), is("127.0.0.2"));
        assertThat(operation.getImageFile(), is(""));
        assertThat(operation.getBrowserUsed(), is("Mosaic"));
        assertThat(operation.getContent(), is("content2"));
        assertThat(operation.getLanguage(), is("EN"));
        assertThat(operation.getLength(), is(2));
        assertThat(operation.getAuthorPersonId(), is(15393162797039l));
        assertThat(operation.getForumId(), is(35393162797039l));
        assertThat(operation.getCountryId(), is(45393162797039l));
        List<Long> tagIds2 = new ArrayList<Long>(){
            {
                add(7509l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds2));
        assertThat(operation.getCreationDate().getTime(), is(1354158101157L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158101157L));

        operation = (LdbcInsert6AddPost) reader.next();
        assertThat(operation.getPostId(), is(2336462234123l));
        assertThat(operation.getLocationIp(), is("127.0.0.3"));
        assertThat(operation.getImageFile(), is("image3"));
        assertThat(operation.getBrowserUsed(), is("Internet Explorer"));
        assertThat(operation.getContent(), is("content3"));
        assertThat(operation.getLanguage(), is("ES"));
        assertThat(operation.getLength(), is(3));
        assertThat(operation.getAuthorPersonId(), is(4398046516485l));
        assertThat(operation.getForumId(), is(3398046516485l));
        assertThat(operation.getCountryId(), is(4398046516485l));
        List<Long> tagIds3 = new ArrayList<Long>(){
            {
                add(2999l); add(125l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds3));
        assertThat(operation.getCreationDate().getTime(), is(1354159101925L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159101925L));

        operation = (LdbcInsert6AddPost) reader.next();
        assertThat(operation.getPostId(), is(2336462255683l));
        assertThat(operation.getLocationIp(), is("127.0.0.4"));
        assertThat(operation.getImageFile(), is(""));
        assertThat(operation.getBrowserUsed(), is("Vivaldi"));
        assertThat(operation.getContent(), is("content4"));
        assertThat(operation.getLanguage(), is("DE"));
        assertThat(operation.getLength(), is(4));
        assertThat(operation.getAuthorPersonId(), is(8796093032781l));
        assertThat(operation.getForumId(), is(3796093032781l));
        assertThat(operation.getCountryId(), is(4796093032781l));
        List<Long> tagIds4 = new ArrayList<Long>(){
            {
                add(1l); add(2l); add(3l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds4));
        assertThat(operation.getCreationDate().getTime(), is(1354159537443L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159537443L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllInsert7Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354157772178L)
            .thenReturn(1354158101157L)
            .thenReturn(1354159101925L)
            .thenReturn(1354159537443L);
        when(rs.getLong(2)) //CommentId
            .thenReturn(2336462220352l)
            .thenReturn(2336462274764l)
            .thenReturn(2336462234123l)
            .thenReturn(2336462255683l);
        when(rs.getString(3))
            .thenReturn("127.0.0.1")
            .thenReturn("127.0.0.2")
            .thenReturn("127.0.0.3")
            .thenReturn("127.0.0.4");
        when(rs.getString(4))
            .thenReturn("Netscape")
            .thenReturn("Mosaic")
            .thenReturn("Internet Explorer")
            .thenReturn("Vivaldi");
        when(rs.getString(5))
            .thenReturn("content1")
            .thenReturn("content2")
            .thenReturn("content3")
            .thenReturn("content4");
        when(rs.getInt(6))
            .thenReturn(1)
            .thenReturn(2)
            .thenReturn(3)
            .thenReturn(4);
        when(rs.getLong(7))
            .thenReturn(2199023256479l)
            .thenReturn(15393162797039l)
            .thenReturn(4398046516485l)
            .thenReturn(8796093032781l);
        when(rs.getLong(8))
            .thenReturn(3199023256479l)
            .thenReturn(35393162797039l)
            .thenReturn(3398046516485l)
            .thenReturn(3796093032781l);
        when(rs.getLong(9))
            .thenReturn(4199023256479l)
            .thenReturn(45393162797039l)
            .thenReturn(4398046516485l)
            .thenReturn(4796093032781l);
        when(rs.getLong(10))
            .thenReturn(5199023256479l)
            .thenReturn(55393162797039l)
            .thenReturn(5398046516485l)
            .thenReturn(5796093032781l);
        when(rs.getString(11))
            .thenReturn("9067")
            .thenReturn("7509")
            .thenReturn("2999;125")
            .thenReturn("1;2;3");

        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderAddComment();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcInsert7AddComment operation;

        operation = (LdbcInsert7AddComment) reader.next();
        assertThat(operation.getCommentId(), is(2336462220352l));
        assertThat(operation.getLocationIp(), is("127.0.0.1"));
        assertThat(operation.getBrowserUsed(), is("Netscape"));
        assertThat(operation.getContent(), is("content1"));
        assertThat(operation.getLength(), is(1));
        assertThat(operation.getAuthorPersonId(), is(2199023256479l));
        assertThat(operation.getCountryId(), is(3199023256479l));
        assertThat(operation.getReplyToPostId(), is(4199023256479l));
        assertThat(operation.getReplyToCommentId(), is(5199023256479l));
        List<Long> tagIds1 = new ArrayList<Long>(){
            {
                add(9067l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds1));
        assertThat(operation.getCreationDate().getTime(), is(1354157772178L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354157772178L));

        operation = (LdbcInsert7AddComment) reader.next();
        assertThat(operation.getCommentId(), is(2336462274764l));
        assertThat(operation.getLocationIp(), is("127.0.0.2"));
        assertThat(operation.getBrowserUsed(), is("Mosaic"));
        assertThat(operation.getContent(), is("content2"));
        assertThat(operation.getLength(), is(2));
        assertThat(operation.getAuthorPersonId(), is(15393162797039l));
        assertThat(operation.getCountryId(), is(35393162797039l));
        assertThat(operation.getReplyToPostId(), is(45393162797039l));
        assertThat(operation.getReplyToCommentId(), is(55393162797039l));
        List<Long> tagIds2 = new ArrayList<Long>(){
            {
                add(7509l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds2));
        assertThat(operation.getCreationDate().getTime(), is(1354158101157L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158101157L));

        operation = (LdbcInsert7AddComment) reader.next();
        assertThat(operation.getCommentId(), is(2336462234123l));
        assertThat(operation.getLocationIp(), is("127.0.0.3"));
        assertThat(operation.getBrowserUsed(), is("Internet Explorer"));
        assertThat(operation.getContent(), is("content3"));
        assertThat(operation.getLength(), is(3));
        assertThat(operation.getAuthorPersonId(), is(4398046516485l));
        assertThat(operation.getCountryId(), is(3398046516485l));
        assertThat(operation.getReplyToPostId(), is(4398046516485l));
        assertThat(operation.getReplyToCommentId(), is(5398046516485l));
        List<Long> tagIds3 = new ArrayList<Long>(){
            {
                add(2999l); add(125l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds3));
        assertThat(operation.getCreationDate().getTime(), is(1354159101925L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159101925L));

        operation = (LdbcInsert7AddComment) reader.next();
        assertThat(operation.getCommentId(), is(2336462255683l));
        assertThat(operation.getLocationIp(), is("127.0.0.4"));
        assertThat(operation.getBrowserUsed(), is("Vivaldi"));
        assertThat(operation.getContent(), is("content4"));
        assertThat(operation.getLength(), is(4));
        assertThat(operation.getAuthorPersonId(), is(8796093032781l));
        assertThat(operation.getCountryId(), is(3796093032781l));
        assertThat(operation.getReplyToPostId(), is(4796093032781l));
        assertThat(operation.getReplyToCommentId(), is(5796093032781l));
        List<Long> tagIds4 = new ArrayList<Long>(){
            {
                add(1l); add(2l); add(3l);
            }
        };
        assertThat(operation.getTagIds(), equalTo(tagIds4));
        assertThat(operation.getCreationDate().getTime(), is(1354159537443L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159537443L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllInsert8Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2))
            .thenReturn(15393162789345l)
            .thenReturn(15393162794683l)
            .thenReturn(8796093026612l)
            .thenReturn(26388279069478l);
        when(rs.getLong(3))
            .thenReturn(2336468703973l)
            .thenReturn(2336467057324l)
            .thenReturn(2336462285980l)
            .thenReturn(2336465315493l);

        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderAddFriendship();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcInsert8AddFriendship operation;

        operation = (LdbcInsert8AddFriendship) reader.next();
        assertThat(operation.getPerson1Id(), is(15393162789345l));
        assertThat(operation.getPerson2Id(), is(2336468703973l));
        assertThat(operation.getCreationDate().getTime(), is(1354158403640L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcInsert8AddFriendship) reader.next();
        assertThat(operation.getPerson1Id(), is(15393162794683l));
        assertThat(operation.getPerson2Id(), is(2336467057324l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert8AddFriendship) reader.next();
        assertThat(operation.getPerson1Id(), is(8796093026612l));
        assertThat(operation.getPerson2Id(), is(2336462285980l));
        assertThat(operation.getCreationDate().getTime(), is(1354159616392L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcInsert8AddFriendship) reader.next();
        assertThat(operation.getPerson1Id(), is(26388279069478l));
        assertThat(operation.getPerson2Id(), is(2336465315493l));
        assertThat(operation.getCreationDate().getTime(), is(1354164952506L));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllDelete1Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354166119784l)
            .thenReturn(1354176134565l)
            .thenReturn(1354223660406l)
            .thenReturn(1354343674368l);
        when(rs.getLong(2))
            .thenReturn(28587302326532l)
            .thenReturn(28587302325029l)
            .thenReturn(8796093027322l)
            .thenReturn(4398046517253l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderDeletePerson();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcDelete1RemovePerson operation;

        operation = (LdbcDelete1RemovePerson) reader.next();
        assertThat(operation.getremovePersonIdD1(), is(28587302326532l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354166119784l));

        operation = (LdbcDelete1RemovePerson) reader.next();
        assertThat(operation.getremovePersonIdD1(), is(28587302325029l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354176134565l));

        operation = (LdbcDelete1RemovePerson) reader.next();
        assertThat(operation.getremovePersonIdD1(), is(8796093027322l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354223660406l));

        operation = (LdbcDelete1RemovePerson) reader.next();
        assertThat(operation.getremovePersonIdD1(), is(4398046517253l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354343674368l));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllDelete2Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2))
            .thenReturn(15393162789345l)
            .thenReturn(15393162794683l)
            .thenReturn(8796093026612l)
            .thenReturn(26388279069478l);
        when(rs.getLong(3))
            .thenReturn(2336468703973l)
            .thenReturn(2336467057324l)
            .thenReturn(2336462285980l)
            .thenReturn(2336465315493l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderDeletePostLike();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcDelete2RemovePostLike operation;

        operation = (LdbcDelete2RemovePostLike) reader.next();
        assertThat(operation.getremovePersonIdD2(), is(15393162789345l));
        assertThat(operation.getremovePostIdD2(), is(2336468703973l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcDelete2RemovePostLike) reader.next();
        assertThat(operation.getremovePersonIdD2(), is(15393162794683l));
        assertThat(operation.getremovePostIdD2(), is(2336467057324l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcDelete2RemovePostLike) reader.next();
        assertThat(operation.getremovePersonIdD2(), is(8796093026612l));
        assertThat(operation.getremovePostIdD2(), is(2336462285980l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcDelete2RemovePostLike) reader.next();
        assertThat(operation.getremovePersonIdD2(), is(26388279069478l));
        assertThat(operation.getremovePostIdD2(), is(2336465315493l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllDelete3Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2))
            .thenReturn(15393162789345l)
            .thenReturn(15393162794683l)
            .thenReturn(8796093026612l)
            .thenReturn(26388279069478l);
        when(rs.getLong(3))
            .thenReturn(2336468703973l)
            .thenReturn(2336467057324l)
            .thenReturn(2336462285980l)
            .thenReturn(2336465315493l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderDeleteCommentLike();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcDelete3RemoveCommentLike operation;

        operation = (LdbcDelete3RemoveCommentLike) reader.next();
        assertThat(operation.getremovePersonIdD3(), is(15393162789345l));
        assertThat(operation.getremoveCommentIdD3(), is(2336468703973l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcDelete3RemoveCommentLike) reader.next();
        assertThat(operation.getremovePersonIdD3(), is(15393162794683l));
        assertThat(operation.getremoveCommentIdD3(), is(2336467057324l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcDelete3RemoveCommentLike) reader.next();
        assertThat(operation.getremovePersonIdD3(), is(8796093026612l));
        assertThat(operation.getremoveCommentIdD3(), is(2336462285980l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcDelete3RemoveCommentLike) reader.next();
        assertThat(operation.getremovePersonIdD3(), is(26388279069478l));
        assertThat(operation.getremoveCommentIdD3(), is(2336465315493l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllDelete4Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354166119784l)
            .thenReturn(1354176134565l)
            .thenReturn(1354223660406l)
            .thenReturn(1354343674368l);
        when(rs.getLong(2))
            .thenReturn(28587302326532l)
            .thenReturn(28587302325029l)
            .thenReturn(8796093027322l)
            .thenReturn(4398046517253l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderDeleteForum();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcDelete4RemoveForum operation;

        operation = (LdbcDelete4RemoveForum) reader.next();
        assertThat(operation.getremoveForumIdD4(), is(28587302326532l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354166119784l));

        operation = (LdbcDelete4RemoveForum) reader.next();
        assertThat(operation.getremoveForumIdD4(), is(28587302325029l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354176134565l));

        operation = (LdbcDelete4RemoveForum) reader.next();
        assertThat(operation.getremoveForumIdD4(), is(8796093027322l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354223660406l));

        operation = (LdbcDelete4RemoveForum) reader.next();
        assertThat(operation.getremoveForumIdD4(), is(4398046517253l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354343674368l));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllDelete5Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2))
            .thenReturn(2336468703973l)
            .thenReturn(2336467057324l)
            .thenReturn(2336462285980l)
            .thenReturn(2336465315493l);
        when(rs.getLong(3))
            .thenReturn(15393162789345l)
            .thenReturn(15393162794683l)
            .thenReturn(8796093026612l)
            .thenReturn(26388279069478l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderDeleteForumMembership();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcDelete5RemoveForumMembership operation;

        operation = (LdbcDelete5RemoveForumMembership) reader.next();
        assertThat(operation.getremovePersonIdD5(), is(15393162789345l));
        assertThat(operation.getremoveForumIdD5(), is(2336468703973l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcDelete5RemoveForumMembership) reader.next();
        assertThat(operation.getremovePersonIdD5(), is(15393162794683l));
        assertThat(operation.getremoveForumIdD5(), is(2336467057324l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcDelete5RemoveForumMembership) reader.next();
        assertThat(operation.getremovePersonIdD5(), is(8796093026612l));
        assertThat(operation.getremoveForumIdD5(), is(2336462285980l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcDelete5RemoveForumMembership) reader.next();
        assertThat(operation.getremovePersonIdD5(), is(26388279069478l));
        assertThat(operation.getremoveForumIdD5(), is(2336465315493l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllDelete6Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354166119784l)
            .thenReturn(1354176134565l)
            .thenReturn(1354223660406l)
            .thenReturn(1354343674368l);
        when(rs.getLong(2))
            .thenReturn(28587302326532l)
            .thenReturn(28587302325029l)
            .thenReturn(8796093027322l)
            .thenReturn(4398046517253l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderDeletePostThread();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcDelete6RemovePostThread operation;

        operation = (LdbcDelete6RemovePostThread) reader.next();
        assertThat(operation.getremovePostIdD6(), is(28587302326532l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354166119784l));

        operation = (LdbcDelete6RemovePostThread) reader.next();
        assertThat(operation.getremovePostIdD6(), is(28587302325029l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354176134565l));

        operation = (LdbcDelete6RemovePostThread) reader.next();
        assertThat(operation.getremovePostIdD6(), is(8796093027322l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354223660406l));

        operation = (LdbcDelete6RemovePostThread) reader.next();
        assertThat(operation.getremovePostIdD6(), is(4398046517253l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354343674368l));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllDelete7Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354166119784l)
            .thenReturn(1354176134565l)
            .thenReturn(1354223660406l)
            .thenReturn(1354343674368l);
        when(rs.getLong(2))
            .thenReturn(28587302326532l)
            .thenReturn(28587302325029l)
            .thenReturn(8796093027322l)
            .thenReturn(4398046517253l);
        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderDeleteCommentSubThread();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcDelete7RemoveCommentSubthread operation;

        operation = (LdbcDelete7RemoveCommentSubthread) reader.next();
        assertThat(operation.getremoveCommentIdD7(), is(28587302326532l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354166119784l));

        operation = (LdbcDelete7RemoveCommentSubthread) reader.next();
        assertThat(operation.getremoveCommentIdD7(), is(28587302325029l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354176134565l));

        operation = (LdbcDelete7RemoveCommentSubthread) reader.next();
        assertThat(operation.getremoveCommentIdD7(), is(8796093027322l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354223660406l));

        operation = (LdbcDelete7RemoveCommentSubthread) reader.next();
        assertThat(operation.getremoveCommentIdD7(), is(4398046517253l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354343674368l));

        assertThat(reader.hasNext(), is(false));
    }

    @Test
    public void shouldParseAllDelete8Events() throws WorkloadException, SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        when(rs.getLong(1))
            .thenReturn(1354158403640L)
            .thenReturn(1354159616392L)
            .thenReturn(1354159616392L)
            .thenReturn(1354164952506L);
        when(rs.getLong(2))
            .thenReturn(15393162789345l)
            .thenReturn(15393162794683l)
            .thenReturn(8796093026612l)
            .thenReturn(26388279069478l);
        when(rs.getLong(3))
            .thenReturn(2336468703973l)
            .thenReturn(2336467057324l)
            .thenReturn(2336462285980l)
            .thenReturn(2336465315493l);

        EventStreamReader.EventDecoder<Operation> decoder = new UpdateEventStreamReader.EventDecoderDeleteFriendship();
        ParquetLoader loader = new ParquetLoader(db);
        Iterator<Operation> opStream = loader.loadOperationStream("/somepath", decoder);

        // Act
        Iterator<Operation> reader = new UpdateEventStreamReader(
            opStream
        );

        // Assert
        LdbcDelete8RemoveFriendship operation;

        operation = (LdbcDelete8RemoveFriendship) reader.next();
        assertThat(operation.getremovePerson1Id(), is(15393162789345l));
        assertThat(operation.getremovePerson2Id(), is(2336468703973l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354158403640L));

        operation = (LdbcDelete8RemoveFriendship) reader.next();
        assertThat(operation.getremovePerson1Id(), is(15393162794683l));
        assertThat(operation.getremovePerson2Id(), is(2336467057324l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcDelete8RemoveFriendship) reader.next();
        assertThat(operation.getremovePerson1Id(), is(8796093026612l));
        assertThat(operation.getremovePerson2Id(), is(2336462285980l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354159616392L));

        operation = (LdbcDelete8RemoveFriendship) reader.next();
        assertThat(operation.getremovePerson1Id(), is(26388279069478l));
        assertThat(operation.getremovePerson2Id(), is(2336465315493l));
        assertThat(operation.scheduledStartTimeAsMilli(), is(1354164952506L));

        assertThat(reader.hasNext(), is(false));
    }
}
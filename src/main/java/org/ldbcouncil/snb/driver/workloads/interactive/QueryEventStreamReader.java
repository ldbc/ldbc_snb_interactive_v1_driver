package org.ldbcouncil.snb.driver.workloads.interactive;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.generator.EventStreamReader.EventDecoder;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;

import static java.lang.String.format;

public class QueryEventStreamReader implements Iterator<Operation>{
    
    private final Iterator<Operation> operationStream;

    public QueryEventStreamReader( Iterator<Operation> operationStream )
    {
        this.operationStream = operationStream;
    }

    @Override
    public boolean hasNext()
    {
        return operationStream.hasNext();
    }

    @Override
    public Operation next()
    {
        Operation query = operationStream.next();
        Operation operation = query.newInstance();
        operation.setDependencyTimeStamp( query.dependencyTimeStamp() );
        operation.setExpiryTimeStamp( query.expiryTimeStamp() );
        return operation;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    public static Map<Integer, EventDecoder<Operation>> getDecoders(){
        Map<Integer, EventDecoder<Operation>> decoders = new HashMap<>();
        decoders.put(LdbcQuery1.TYPE, new Query1Decoder());
        decoders.put(LdbcQuery2.TYPE, new Query2Decoder());
        decoders.put(LdbcQuery3a.TYPE, new Query3aDecoder());
        decoders.put(LdbcQuery3b.TYPE, new Query3bDecoder());
        decoders.put(LdbcQuery4.TYPE, new Query4Decoder());
        decoders.put(LdbcQuery5.TYPE, new Query5Decoder());
        decoders.put(LdbcQuery6.TYPE, new Query6Decoder());
        decoders.put(LdbcQuery7.TYPE, new Query7Decoder());
        decoders.put(LdbcQuery8.TYPE, new Query8Decoder());
        decoders.put(LdbcQuery9.TYPE, new Query9Decoder());
        decoders.put(LdbcQuery10.TYPE, new Query10Decoder());
        decoders.put(LdbcQuery11.TYPE, new Query11Decoder());
        decoders.put(LdbcQuery12.TYPE, new Query12Decoder());
        decoders.put(LdbcQuery13a.TYPE, new Query13aDecoder());
        decoders.put(LdbcQuery13b.TYPE, new Query13bDecoder());
        decoders.put(LdbcQuery14a.TYPE, new Query14aDecoder());
        decoders.put(LdbcQuery14b.TYPE, new Query14bDecoder());
        return decoders;
    }

    public static class Query1Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery1 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                String personName = rs.getString(2);
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery1(
                    personId,
                    personName,
                        LdbcQuery10.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query1Event: %s", e));
            }
        }
    }

    public static class Query2Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery2 Object 
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try {
                long personId = rs.getLong(1);
                Date maxDate = new Date(rs.getTimestamp(2).getTime());//convertStringToDate(rs.getString(2));
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery2(
                    personId,
                    maxDate,
                    LdbcQuery2.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query2Event: %s", e));
            }
        }
    }

    public static class Query3aDecoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery3 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                String countryXName = rs.getString(2);
                String countryYName = rs.getString(3);
                Date maxDate = new Date(rs.getTimestamp(4).getTime());//convertStringToDate(rs.getString(4));
                int durationDays = rs.getInt(5);
                long dependencyTimeStamp = convertStringToLong(rs.getString(6));
                long expiryTimeStamp = convertStringToLong(rs.getString(7));
                Operation query = new LdbcQuery3a(
                    personId,
                    countryXName,
                    countryYName,
                    maxDate,
                    durationDays,
                    LdbcQuery3a.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query3aEvent: %s", e));
            }
        }
    }

    public static class Query3bDecoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery3 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                String countryXName = rs.getString(2);
                String countryYName = rs.getString(3);
                Date maxDate = new Date(rs.getTimestamp(4).getTime());//convertStringToDate(rs.getString(4));
                int durationDays = rs.getInt(5);
                long dependencyTimeStamp = convertStringToLong(rs.getString(6));
                long expiryTimeStamp = convertStringToLong(rs.getString(7));
                Operation query = new LdbcQuery3b(
                    personId,
                    countryXName,
                    countryYName,
                    maxDate,
                    durationDays,
                    LdbcQuery3b.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query3bEvent: %s", e));
            }
        }
    }

    public static class Query4Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery4 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                Date startDate = new Date(rs.getTimestamp(2).getTime());//convertStringToDate(rs.getString(2));
                int durationDays = rs.getInt(3);
                long dependencyTimeStamp = convertStringToLong(rs.getString(4));
                long expiryTimeStamp = convertStringToLong(rs.getString(5));
                Operation query = new LdbcQuery4(
                    personId,
                    startDate,
                    durationDays,
                    LdbcQuery4.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query4Event: %s", e));
            }
        }
    }

    public static class Query5Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery5 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try {
                long personId = rs.getLong(1);
                // Dates are stored as long in the oepration streams.
                Date minDate = new Date(rs.getTimestamp(2).getTime());//convertStringToDate(rs.getString(2));
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery5(
                    personId,
                    minDate,
                    LdbcQuery5.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query5Event: %s", e));
            }
        }
    }

    public static class Query6Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery6 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                String personName = rs.getString(2);
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery6(
                    personId,
                    personName,
                    LdbcQuery6.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query6Event: %s", e));
            }
        }
    }

    public static class Query7Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery7 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                long dependencyTimeStamp = convertStringToLong(rs.getString(2));
                long expiryTimeStamp = convertStringToLong(rs.getString(3));
                Operation query = new LdbcQuery7(
                    personId,
                    LdbcQuery7.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query7Event: %s", e));
            }
        }
    }

    public static class Query8Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery8 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                long dependencyTimeStamp = convertStringToLong(rs.getString(2));
                long expiryTimeStamp = convertStringToLong(rs.getString(3));
                Operation query = new LdbcQuery8(
                    personId,
                    LdbcQuery8.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query8Event: %s", e));
            }
        }
    }

    public static class Query9Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery9 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try {
                long personId = rs.getLong(1);
                Date maxDate = new Date(rs.getTimestamp(2).getTime());//convertStringToDate(rs.getString(2));
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery9(
                    personId,
                    maxDate,
                    LdbcQuery9.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query9Event: %s", e));
            }
        }
    }

    public static class Query10Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery10 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                int personName = rs.getInt(2);
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery10(
                    personId,
                    personName,
                    LdbcQuery10.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query10Event: %s", e));
            }
        }
    }

    public static class Query11Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery11 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                String countryName = rs.getString(2);
                int workFromYear = rs.getInt(3);
                long dependencyTimeStamp = convertStringToLong(rs.getString(4));
                long expiryTimeStamp = convertStringToLong(rs.getString(5));
                Operation query = new LdbcQuery11(
                    personId,
                    countryName,
                    workFromYear,
                    LdbcQuery11.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query11Event: %s", e));
            }
        }
    }

    public static class Query12Decoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery12 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                String tagClassName = rs.getString(2);
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery12(
                    personId,
                    tagClassName,
                    LdbcQuery12.DEFAULT_LIMIT
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query12Event: %s", e));
            }
        }
    }

    public static class Query13aDecoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery13 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId1 = rs.getLong(1);
                long personId2 = rs.getLong(2);
                long dependencyTimeStamp = rs.getLong(3);
                long expiryTimeStamp = rs.getLong(4);
                Operation query = new LdbcQuery13a(
                    personId1,
                    personId2
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query13aEvent: %s", e));
            }
        }
    }

    public static class Query13bDecoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery13 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId1 = rs.getLong(1);
                long personId2 = rs.getLong(2);
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery13b(
                    personId1,
                    personId2
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query13bEvent: %s", e));
            }
        }
    }

    public static class Query14aDecoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery14 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId1 = rs.getLong(1);
                long personId2 = rs.getLong(2);
                long dependencyTimeStamp = rs.getLong(3);
                long expiryTimeStamp = rs.getLong(4);
                Operation query = new LdbcQuery14a(
                    personId1,
                    personId2
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query14aEvent: %s", e));
            }
        }
    }

    public static class Query14bDecoder implements EventDecoder<Operation>
    {
        /**
         * @param rs: Resultset object containing the row to decode
         * @return LdbcQuery14 Object
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId1 = rs.getLong(1);
                long personId2 = rs.getLong(2);
                long dependencyTimeStamp = convertStringToLong(rs.getString(3));
                long expiryTimeStamp = convertStringToLong(rs.getString(4));
                Operation query = new LdbcQuery14b(
                    personId1,
                    personId2
                );
                query.setDependencyTimeStamp(dependencyTimeStamp);
                query.setExpiryTimeStamp(expiryTimeStamp);
                return query;
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query14bEvent: %s", e));
            }
        }
    }

    private static long convertStringToLong(String dateString)
    {
        return Instant.parse(dateString.replace(" ", "T") + "Z").toEpochMilli();
    }
}

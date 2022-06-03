package org.ldbcouncil.snb.driver.workloads.interactive;


import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.QueryEventStreamDecoder;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery3;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import static java.lang.String.format;

public class Query3EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> csvRows;

    public Query3EventStreamReader( Iterator<Object[]> csvRows )
    {
        this.csvRows = csvRows;
    }

    @Override
    public boolean hasNext()
    {
        return csvRows.hasNext();
    }

    @Override
    public Operation next()
    {
        Object[] rowAsObjects = csvRows.next();
        Operation operation = new LdbcQuery3(
                (long) rowAsObjects[0],
                (String) rowAsObjects[3],
                (String) rowAsObjects[4],
                (Date) rowAsObjects[1],
                (int) rowAsObjects[2],
                LdbcQuery3.DEFAULT_LIMIT
        );
        operation.setDependencyTimeStamp( 0 );
        return operation;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    /**
     * Inner class used for decoding Resultset data for query 3 parameters.
     */
    public static class QueryDecoder implements QueryEventStreamDecoder.EventDecoder<Object[]>
    {
    //     personId|startDate|durationDays|countryXName|countryYName
    //     7696581543848|1293840000|28|Egypt|Sri_Lanka

        /**
         * @param rs: Resultset object containing the row to decode
        * @return Object array
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Object[] decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId = rs.getLong(1);
                Date maxDate = new Date(rs.getLong(2));
                int durationDays = rs.getInt(3);
                String countryXName = rs.getString(4);
                String countryYName = rs.getString(5);
                return new Object[]{personId, maxDate, durationDays, countryXName, countryYName};
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query1Event: %s", e));
            }
        }
    }
}

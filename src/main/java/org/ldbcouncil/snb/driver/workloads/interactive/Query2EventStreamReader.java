package org.ldbcouncil.snb.driver.workloads.interactive;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.QueryEventStreamDecoder;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import static java.lang.String.format;

public class Query2EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> eventArray;

    public Query2EventStreamReader( Iterator<Object[]> eventArray )
    {
        this.eventArray = eventArray;
    }

    @Override
    public boolean hasNext()
    {
        return eventArray.hasNext();
    }

    @Override
    public Operation next()
    {
        Object[] rowAsObjects = eventArray.next();
        Operation operation = new LdbcQuery2(
                (long) rowAsObjects[0],
                (Date) rowAsObjects[1],
                LdbcQuery2.DEFAULT_LIMIT
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
     * Inner class used for decoding Resultset data for query 2 parameters.
     */
    public static class QueryDecoder implements QueryEventStreamDecoder.EventDecoder<Object[]>
    {
        // personId|maxDate
        // 1236219|1335225600

        /**
         * @param rs: Resultset object containing the row to decode
         * @return Object array (TODO: change Object[] to LdbcQuery2)
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Object[] decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try {
                long personId = rs.getLong(1);
                // Dates are stored as long in the operation streams.
                Date maxDate = new Date(rs.getLong(2));
                return new Object[]{personId, maxDate};
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query2Event: %s", e));
            }
        }
    }
}

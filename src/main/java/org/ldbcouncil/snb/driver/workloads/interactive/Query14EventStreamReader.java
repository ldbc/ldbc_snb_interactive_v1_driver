package org.ldbcouncil.snb.driver.workloads.interactive;


import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.QueryEventStreamDecoder;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery14;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import static java.lang.String.format;

public class Query14EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> csvRows;

    public Query14EventStreamReader( Iterator<Object[]> csvRows )
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
        Operation operation = new LdbcQuery14(
                (long) rowAsObjects[0],
                (long) rowAsObjects[1]
        );
        operation.setDependencyTimeStamp( 0 );
        return operation;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    public static class QueryDecoder implements QueryEventStreamDecoder.EventDecoder<Object[]>
    {
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
                long personId1 = rs.getLong(1);
                long personId2 = rs.getLong(2);
                return new Object[]{personId1, personId2};
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query1Event: %s", e));
            }
        }
    }
}

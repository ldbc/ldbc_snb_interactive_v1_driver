package org.ldbcouncil.snb.driver.workloads.interactive;


import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.QueryEventStreamReader;
import org.ldbcouncil.snb.driver.generator.GeneratorException;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery5;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import static java.lang.String.format;

public class Query5EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Object[]> csvRows;

    public Query5EventStreamReader( Iterator<Object[]> csvRows )
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
        Operation operation = new LdbcQuery5(
                (long) rowAsObjects[0],
                (Date) rowAsObjects[1],
                LdbcQuery5.DEFAULT_LIMIT
        );
        operation.setDependencyTimeStamp( 0 );
        return operation;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }

    public static class QueryDecoder implements QueryEventStreamReader.EventDecoder<Object[]>
    {
        // personId|minDate
        // 1236219|1335225600
        /**
         * @param rs: Resultset object containing the row to decode
         * @return Object array 
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Object[] decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try {
                long personId = rs.getLong(1);
                // Dates are stored as long in the oepration streams.
                Date minDate = new Date(rs.getLong(2));
                return new Object[]{personId, minDate};
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query2Event: %s", e));
            }
        }
    }
}

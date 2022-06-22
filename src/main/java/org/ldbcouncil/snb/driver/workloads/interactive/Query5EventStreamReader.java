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
    private final Iterator<Operation> objectArray;

    public Query5EventStreamReader( Iterator<Operation> objectArray )
    {
        this.objectArray = objectArray;
    }

    @Override
    public boolean hasNext()
    {
        return objectArray.hasNext();
    }

    @Override
    public Operation next()
    {
        LdbcQuery5 query = (LdbcQuery5) objectArray.next();
        Operation operation = new LdbcQuery5(query);
        operation.setDependencyTimeStamp( 0 );
        return operation;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }


    public static class QueryDecoder implements QueryEventStreamReader.EventDecoder<Operation>
    {
        // personId|minDate
        // 1236219|1335225600
        /**
         * @param rs: Resultset object containing the row to decode
         * @return Object array 
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try {
                long personId = rs.getLong(1);
                // Dates are stored as long in the oepration streams.
                Date minDate = new Date(rs.getLong(2));
                return new LdbcQuery5(
                    personId,
                    minDate,
                    LdbcQuery5.DEFAULT_LIMIT
            );
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query2Event: %s", e));
            }
        }
    }
}

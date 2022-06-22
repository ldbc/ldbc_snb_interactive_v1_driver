package org.ldbcouncil.snb.driver.workloads.interactive;


import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.QueryEventStreamReader;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery14;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import static java.lang.String.format;

public class Query14EventStreamReader implements Iterator<Operation>
{
    private final Iterator<Operation> objectArray;

    public Query14EventStreamReader( Iterator<Operation> objectArray )
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
        LdbcQuery14 query = (LdbcQuery14) objectArray.next();
        Operation operation = new LdbcQuery14(query);
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
        /**
         * @param rs: Resultset object containing the row to decode
         * @return Object array
         * @throws SQLException when an error occurs reading the resultset
         */
        @Override
        public Operation decodeEvent( ResultSet rs ) throws WorkloadException
        {
            try
            {
                long personId1 = rs.getLong(1);
                long personId2 = rs.getLong(2);
                return new LdbcQuery14(
                    personId1,
                    personId2
                );
            }
            catch (SQLException e){
                throw new WorkloadException(format("Error while decoding ResultSet for Query1Event: %s", e));
            }
        }
    }
}

package org.ldbcouncil.snb.driver.generator;

import java.sql.ResultSet;
import org.ldbcouncil.snb.driver.WorkloadException;

public class QueryEventStreamReader<BASE_EVENT_TYPE>
{
    public interface EventDecoder<BASE_EVENT_TYPE>
    {
        BASE_EVENT_TYPE decodeEvent( ResultSet rs )
                throws WorkloadException;
    }
}

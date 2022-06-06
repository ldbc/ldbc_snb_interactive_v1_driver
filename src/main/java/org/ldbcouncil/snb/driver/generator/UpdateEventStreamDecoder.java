package org.ldbcouncil.snb.driver.generator;

import java.sql.ResultSet;

import org.ldbcouncil.snb.driver.WorkloadException;

public class UpdateEventStreamDecoder<Operation>
{
    public interface UpdateEventDecoder<Operation>
    {
        Operation decodeEvent( ResultSet rs )
                throws WorkloadException;
    }
}

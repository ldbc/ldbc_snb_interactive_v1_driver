package org.ldbcouncil.snb.driver.generator;

public class UpdateEventStreamDecoder<Operation>
{
    public interface UpdateEventDecoder<Operation>
    {
        Operation decodeEvent( String[] rowAsArray );
    }
}

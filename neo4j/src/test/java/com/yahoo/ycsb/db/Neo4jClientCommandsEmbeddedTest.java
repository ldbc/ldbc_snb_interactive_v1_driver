package com.yahoo.ycsb.db;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.impl.util.FileUtils;

import com.yahoo.ycsb.DBException;

public class Neo4jClientCommandsEmbeddedTest extends Neo4jClientCommandsTest
{
    @Override
    public Neo4jClientCommands getClientCommandsImpl() throws DBException
    {
        try
        {
            FileUtils.deleteRecursively( new File( "/tmp/db" ) );
            return new Neo4jClientCommandsEmbedded( "/tmp/db", PRIMARY_KEY );
        }
        catch ( IOException e )
        {
            throw new DBException( "Could not create Neo4jClientCommmandsEmbedded", e.getCause() );
        }
    }
}

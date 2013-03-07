package com.yahoo.ycsb.db;

import org.junit.Ignore;

@Ignore
public class Neo4jClientCommandsRestTest extends Neo4jClientCommandsTest
{
    @Override
    public Neo4jClientCommands getClientCommandsImpl()
    {
        return new Neo4jClientCommandsRest( "http://localhost:7474/db/data", PRIMARY_KEY );
    }
}

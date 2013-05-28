package com.ldbc.driver.db.neo4j;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.kernel.impl.util.FileUtils;

import OLD_com.ldbc.driver.DBException;

import com.ldbc.driver.db.neo4j.Neo4jClientCommands;
import com.ldbc.driver.db.neo4j.Neo4jClientCommandsEmbedded;

public class Neo4jClientCommandsEmbeddedTest extends Neo4jClientCommandsTest
{
    @Override
    public Neo4jClientCommands getClientCommandsImpl() throws DBException
    {
        return new Neo4jClientCommandsEmbedded( "/tmp/db", PRIMARY_KEY );
    }

    @BeforeClass
    public static void cleanSlateBeforeTests() throws DBException
    {
        deleteDatabase();
    }

    @AfterClass
    public static void cleanSlateAfterTests() throws DBException
    {
        deleteDatabase();
    }

    private static void deleteDatabase() throws DBException
    {
        try
        {
            FileUtils.deleteRecursively( new File( "/tmp/db" ) );
        }
        catch ( IOException e )
        {
            throw new DBException( "Could not create Neo4jClientCommmandsEmbedded", e.getCause() );
        }
    }
}

package com.ldbc.db.neo4j;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ldbc.DBException;
import com.ldbc.data.ByteIterator;

public interface Neo4jClientCommands
{
    public void init();

    public void cleanUp();

    public Map<String, ByteIterator> read( String table, String key, Set<String> fields ) throws DBException;

    public Vector<Map<String, ByteIterator>> scan( String table, String startkey, int recordcount, Set<String> fields )
            throws DBException;

    public void update( String table, String key, Map<String, ByteIterator> values ) throws DBException;

    public void insert( String table, String key, Map<String, ByteIterator> values ) throws DBException;

    public void delete( String table, String key ) throws DBException;

    public void clearDb() throws DBException;

    public long nodeCount() throws DBException;

    public long relationshipCount() throws DBException;
}

package com.yahoo.ycsb.db;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.yahoo.ycsb.DBException;

public class Neo4jClientLogger
{

    public void error( String msg ) throws DBException
    {
        error( msg, null );
    }

    public void error( String msg, Exception e ) throws DBException
    {
        msg = "[Neo4jClient] " + msg;
        final String exceptionMsg = ( null != e ) ? exceptionToString( e ) : "";
        throw new DBException( msg + "\n" + exceptionMsg );
    }

    public void debug( String msg )
    {
        debug( msg, null );
    }

    public void debug( String msg, Exception e )
    {
        msg = "[Neo4jClient] " + msg;
        final String exceptionMsg = ( null != e ) ? exceptionToString( e ) : "";
        System.err.println( msg + "\n" + exceptionMsg );
    }

    public void info( String msg )
    {
        System.out.println( "[Neo4jClient] " + msg );
    }

    private String exceptionToString( Exception e )
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        e.printStackTrace( pw );
        return sw.toString();
    }
}

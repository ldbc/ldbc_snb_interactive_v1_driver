package com.ldbc.driver.runtime;

import com.google.common.collect.ImmutableList;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

// TODO rewrite like sync GCT tracker class, to be more threadsafe
public class ConcurrentErrorReporter
{
    public static String stackTraceToString( Throwable e )
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        e.printStackTrace( pw );
        return sw.toString();
    }

    public static String whoAmI( Object caller )
    {
        Thread myThread = Thread.currentThread();
        return format( "%s [%s] (Thread: ID=%s, Name=%s, Priority=%s)",
                caller.getClass().getSimpleName(),
                Thread.currentThread().getStackTrace()[3].getLineNumber(),
                myThread.getId(),
                myThread.getName(),
                myThread.getPriority() );
    }

    public static String formatErrors( List<ErrorReport> errors )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "\n- Start Error Log -" );
        // Do this to avoid ConcurrentModificationException in case error is reported while iterating through errors
        Iterator<ErrorReport> errorsIterator = ImmutableList.copyOf( errors ).iterator();
        while ( errorsIterator.hasNext() )
        {
            ErrorReport error = errorsIterator.next();
            sb.append( "\n\tSOURCE:\t" ).append( error.source() );
            sb.append( "\n\tERROR:\t" ).append( error.error() );
        }
        sb.append( "\n- End Error Log -\n" );
        return sb.toString();
    }

    private final List<ErrorReport> errorMessages = new ArrayList<>();

    synchronized public void reportError( Object caller, String errMsg )
    {
        syncGetErrorMessages().add( new ErrorReport( whoAmI( caller ), errMsg ) );
    }

    public boolean errorEncountered()
    {
        return !syncGetErrorMessages().isEmpty();
    }

    public List<ErrorReport> errorMessages()
    {
        return syncGetErrorMessages();
    }

    @Override
    public String toString()
    {
        if ( syncGetErrorMessages().isEmpty() )
        {
            return "No Reported Errors";
        }
        else
        {
            return formatErrors( syncGetErrorMessages() );
        }
    }

    private synchronized List<ErrorReport> syncGetErrorMessages()
    {
        return errorMessages;
    }

    public static class ErrorReport
    {
        private final String source;
        private final String error;

        public ErrorReport( String source, String error )
        {
            this.source = source;
            this.error = error;
        }

        public String source()
        {
            return source;
        }

        public String error()
        {
            return error;
        }
    }
}

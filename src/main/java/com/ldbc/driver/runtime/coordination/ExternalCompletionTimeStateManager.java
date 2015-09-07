package com.ldbc.driver.runtime.coordination;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class ExternalCompletionTimeStateManager implements ExternalCompletionTimeReader, ExternalCompletionTimeWriter
{
    private final Map<String,Long> peerCompletionTimesAsMilli = new HashMap<>();
    private long completionTimeAsMilli = -1;
    private boolean notModifiedSinceLastGet = false;

    ExternalCompletionTimeStateManager( Set<String> peerIds ) throws CompletionTimeException
    {
        for ( String peerId : peerIds )
        {
            if ( null == peerId )
            { throw new CompletionTimeException( format( "Peer ID cannot be null\n%s", peerIds.toString() ) ); }
            peerCompletionTimesAsMilli.put( peerId, -1l );
        }
    }

    @Override
    public void submitPeerCompletionTime( String peerId, long timeAsMilli ) throws CompletionTimeException
    {
        if ( null == peerId )
        { throw new CompletionTimeException( "Peer ID can not be null" ); }
        if ( -1 == timeAsMilli )
        { throw new CompletionTimeException( "Invalid completion time " + timeAsMilli ); }
        if ( false == peerCompletionTimesAsMilli.containsKey( peerId ) )
        { throw new CompletionTimeException( format( "Unrecognized peer ID: %s", peerId ) ); }
        long previousPeerCompletionTimeAsMilli = peerCompletionTimesAsMilli.get( peerId );
        if ( -1 != previousPeerCompletionTimeAsMilli && timeAsMilli < previousPeerCompletionTimeAsMilli )
        {
            throw new CompletionTimeException(
                    format(
                            "Completion Time received from Peer(%s) is not monotonically increasing\n"
                            + "  Previous Completion Time: %s\n"
                            + "  Current Completion Time: %s",
                            peerId,
                            timeAsMilli,
                            previousPeerCompletionTimeAsMilli ) );
        }
        notModifiedSinceLastGet = false;
        peerCompletionTimesAsMilli.put( peerId, timeAsMilli );
    }

    @Override
    public long externalCompletionTimeAsMilli()
    {
        if ( notModifiedSinceLastGet )
        { return completionTimeAsMilli; }

        notModifiedSinceLastGet = true;

        completionTimeAsMilli = minPeerCompletionTimeOrNegativeOne();
        return completionTimeAsMilli;
    }

    private long minPeerCompletionTimeOrNegativeOne()
    {
        long externalCompletionTimeAsMilli = -1;
        for ( long peerCompletionTimeAsMilli : peerCompletionTimesAsMilli.values() )
        {
            if ( -1 == peerCompletionTimeAsMilli )
            { return -1; }
            if ( -1 == externalCompletionTimeAsMilli )
            { externalCompletionTimeAsMilli = peerCompletionTimeAsMilli; }
            else if ( peerCompletionTimeAsMilli < externalCompletionTimeAsMilli )
            { externalCompletionTimeAsMilli = peerCompletionTimeAsMilli; }
        }
        return externalCompletionTimeAsMilli;
    }
}

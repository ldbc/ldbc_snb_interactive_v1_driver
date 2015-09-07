package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class PeerCommunicatorThread extends Thread
{
    private final TimeSource timeSource;

    // TODO heartbeat failure detection
    // TODO need separate non-Thread class to do that

    // TODO temporary until Akka/network is integrated
    // TODO need dummy PeerThread that just sends back the Completion Time it received
    private final BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent> peerReceiveQueue;
    private final List<BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent>> peerSendQueues;

    private final static long PEER_RECEIVE_QUEUE_POLL_TIMEOUT_AS_MILLI = 100;

    private final String myId;
    private final ConcurrentErrorReporter errorReporter;
    private final long heartbeatPeriodAsMilli;
    private final AtomicBoolean terminate;
    private final AtomicLong sharedLctReference;
    private final BlockingQueue<CompletionTimeEvent> completionTimeQueue;

    private long lastHeartbeatAsMilli;


    public PeerCommunicatorThread( TimeSource timeSource,
            String myId,
            ConcurrentErrorReporter errorReporter,
            long heartbeatPeriodAsMilli,
            AtomicBoolean terminate,
            AtomicLong sharedLctReference,
            BlockingQueue<CompletionTimeEvent> completionTimeQueue,
            // TODO temporary until Akka/network is integrated
            BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent> peerReceiveQueue,
            List<BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent>> peerSendQueues )
    {
        super( PeerCommunicatorThread.class.getSimpleName() + "-" + System.currentTimeMillis() );
        this.timeSource = timeSource;
        this.myId = myId;
        this.errorReporter = errorReporter;
        this.heartbeatPeriodAsMilli = heartbeatPeriodAsMilli;
        this.terminate = terminate;
        this.sharedLctReference = sharedLctReference;
        this.completionTimeQueue = completionTimeQueue;
        // TODO temporary until Akka/network is integrated
        this.peerReceiveQueue = peerReceiveQueue;
        this.peerSendQueues = peerSendQueues;
        // to force immediate transfer of local completion time
        lastHeartbeatAsMilli = this.timeSource.nowAsMilli() - heartbeatPeriodAsMilli;
    }

    @Override
    public void run()
    {
        while ( false == terminate.get() )
        {
            try
            {
                if ( timeSource.nowAsMilli() - lastHeartbeatAsMilli > heartbeatPeriodAsMilli )
                {
                    sendCompletionTimeToPeers();
                    lastHeartbeatAsMilli = timeSource.nowAsMilli();
                }
                CompletionTimeEvent.ExternalCompletionTimeEvent event =
                        peerReceiveQueue.poll( PEER_RECEIVE_QUEUE_POLL_TIMEOUT_AS_MILLI, TimeUnit.MILLISECONDS );
                if ( null != event )
                { completionTimeQueue.put( event ); }
            }
            catch ( InterruptedException e )
            {
                errorReporter.reportError(
                        this,
                        format( "Thread was interrupted" ) );
                break;
            }
        }
    }

    private void sendCompletionTimeToPeers() throws InterruptedException
    {
        long ct = sharedLctReference.get();
        for ( BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent> peerSendChannel : peerSendQueues )
        { CompletionTimeEvent.writeExternalCompletionTime( myId, ct ); }
    }
}
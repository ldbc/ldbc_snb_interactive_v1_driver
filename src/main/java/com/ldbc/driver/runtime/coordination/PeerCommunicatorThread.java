package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PeerCommunicatorThread extends Thread {
    private final TimeSource timeSource;

    // TODO heartbeat failure detection
    // TODO need separate non-Thread class to do that

    // TODO temporary until Akka/network is integrated
    // TODO need dummy PeerThread that just sends back the Completion Time it received
    private final BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent> peerReceiveQueue;
    private final List<BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent>> peerSendQueues;

    private final static Time PEER_RECEIVE_QUEUE_POLL_TIMEOUT = Time.fromMilli(100);

    private final String myId;
    private final ConcurrentErrorReporter errorReporter;
    private final long heartbeatPeriodAsMilli;
    private final AtomicBoolean terminate;
    private final AtomicReference<Time> sharedLctReference;
    private final BlockingQueue<CompletionTimeEvent> completionTimeQueue;

    private long lastHeartbeatAsMilli;


    public PeerCommunicatorThread(TimeSource timeSource,
                                  String myId,
                                  ConcurrentErrorReporter errorReporter,
                                  Duration heartbeatPeriod,
                                  AtomicBoolean terminate,
                                  AtomicReference<Time> sharedLctReference,
                                  BlockingQueue<CompletionTimeEvent> completionTimeQueue,
                                  // TODO temporary until Akka/network is integrated
                                  BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent> peerReceiveQueue,
                                  List<BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent>> peerSendQueues) {
        super(PeerCommunicatorThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.timeSource = timeSource;
        this.myId = myId;
        this.errorReporter = errorReporter;
        this.heartbeatPeriodAsMilli = heartbeatPeriod.asMilli();
        this.terminate = terminate;
        this.sharedLctReference = sharedLctReference;
        this.completionTimeQueue = completionTimeQueue;
        // TODO temporary until Akka/network is integrated
        this.peerReceiveQueue = peerReceiveQueue;
        this.peerSendQueues = peerSendQueues;
        // to force immediate transfer of local completion time
        lastHeartbeatAsMilli = this.timeSource.now().minus(heartbeatPeriod).asMilli();
    }

    @Override
    public void run() {
        while (false == terminate.get()) {
            try {
                if (timeSource.nowAsMilli() - lastHeartbeatAsMilli > heartbeatPeriodAsMilli) {
                    sendCompletionTimeToPeers();
                    lastHeartbeatAsMilli = timeSource.nowAsMilli();
                }
                CompletionTimeEvent.ExternalCompletionTimeEvent event = peerReceiveQueue.poll(PEER_RECEIVE_QUEUE_POLL_TIMEOUT.asMilli(), TimeUnit.MILLISECONDS);
                if (null != event)
                    completionTimeQueue.put(event);
            } catch (InterruptedException e) {
                errorReporter.reportError(
                        this,
                        String.format("Thread was interrupted"));
                break;
            }
        }
    }

    private void sendCompletionTimeToPeers() throws InterruptedException {
        Time ct = sharedLctReference.get();
        for (BlockingQueue<CompletionTimeEvent.ExternalCompletionTimeEvent> peerSendChannel : peerSendQueues)
            CompletionTimeEvent.writeExternalCompletionTime(myId, ct);
    }
}
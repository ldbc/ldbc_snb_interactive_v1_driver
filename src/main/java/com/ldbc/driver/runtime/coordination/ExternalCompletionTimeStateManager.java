package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ExternalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.ExternalCompletionTimeWriter;
import com.ldbc.driver.temporal.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ExternalCompletionTimeStateManager implements ExternalCompletionTimeReader, ExternalCompletionTimeWriter {
    private final Map<String, Time> peerCompletionTimes = new HashMap<>();
    private Time completionTime = null;
    private boolean notModifiedSinceLastGet = false;

    ExternalCompletionTimeStateManager(Set<String> peerIds) throws CompletionTimeException {
        for (String peerId : peerIds) {
            if (null == peerId)
                throw new CompletionTimeException(String.format("Peer ID cannot be null\n%s", peerIds.toString()));
            peerCompletionTimes.put(peerId, null);
        }
    }

    @Override
    public void submitPeerCompletionTime(String peerId, Time peerCompletionTime) throws CompletionTimeException {
        if (null == peerId)
            throw new CompletionTimeException("Peer ID can not be null");
        if (null == peerCompletionTime)
            throw new CompletionTimeException("Completion time can not be null");
        if (false == peerCompletionTimes.containsKey(peerId))
            throw new CompletionTimeException(String.format("Unrecognized peer ID: %s", peerId));
        Time previousPeerCompletionTime = peerCompletionTimes.get(peerId);
        if (null != previousPeerCompletionTime && peerCompletionTime.lt(previousPeerCompletionTime))
            throw new CompletionTimeException(
                    String.format(
                            "Completion Time received from Peer(%s) is not monotonically increasing\n"
                                    + "  Previous Completion Time: %s\n"
                                    + "  Current Completion Time: %s",
                            peerId,
                            peerCompletionTime,
                            previousPeerCompletionTime));
        notModifiedSinceLastGet = false;
        peerCompletionTimes.put(peerId, peerCompletionTime);
    }

    @Override
    public Time externalCompletionTime() {
        if (notModifiedSinceLastGet)
            return completionTime;

        notModifiedSinceLastGet = true;

        completionTime = minPeerCompletionTimeOrNull();
        return completionTime;
    }

    private Time minPeerCompletionTimeOrNull() {
        Time externalCompletionTime = null;
        for (Time peerCompletionTime : peerCompletionTimes.values()) {
            if (null == peerCompletionTime) return null;
            if (null == externalCompletionTime)
                externalCompletionTime = peerCompletionTime;
            else if (peerCompletionTime.lt(externalCompletionTime))
                externalCompletionTime = peerCompletionTime;
        }
        return externalCompletionTime;
    }
}

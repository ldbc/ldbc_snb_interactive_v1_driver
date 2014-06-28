package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalCompletionTime {
    private final Map<String, Time> peerCompletionTimes = new HashMap<String, Time>();
    private final Set<String> peerIds;
    private Time completionTime = null;
    private boolean notModifiedSinceLastGet = false;

    ExternalCompletionTime(Set<String> peerIds) throws CompletionTimeException {
        for (String peerId : peerIds) {
            if (null == peerId)
                throw new CompletionTimeException(String.format("Peer ID cannot be null\n%s", peerIds.toString()));
            peerCompletionTimes.put(peerId, null);
        }
        this.peerIds = peerIds;
    }

    void applyPeerCompletionTime(String peerId, Time peerCompletionTime) throws CompletionTimeException {
        if (null == peerId)
            throw new CompletionTimeException("Peer ID can not be null");
        if (null == peerCompletionTime)
            throw new CompletionTimeException("Completion time can not be null");
        if (false == peerCompletionTimes.containsKey(peerId))
            throw new CompletionTimeException(String.format("Unrecognized peer ID: %s", peerId));
        notModifiedSinceLastGet = false;
        peerCompletionTimes.put(peerId, peerCompletionTime);
    }

    Time completionTime() {
        if (notModifiedSinceLastGet)
            return completionTime;

        notModifiedSinceLastGet = true;

        completionTime = minPeerCompletionTimeOrNull();
        return completionTime;
    }

    Set<String> peersIds() {
        return peerIds;
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

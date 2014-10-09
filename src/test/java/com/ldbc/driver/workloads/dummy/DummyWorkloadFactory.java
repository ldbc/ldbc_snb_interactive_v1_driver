package com.ldbc.driver.workloads.dummy;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.validation.WorkloadFactory;

import java.util.Iterator;
import java.util.List;

public class DummyWorkloadFactory implements WorkloadFactory {
    private final Iterator<WorkloadStreams> streams;
    private final Iterator<Operation<?>> alternativeLastOperations;
    private final Duration maxExpectedInterleave;

    public DummyWorkloadFactory(Iterator<WorkloadStreams> streams,
                                Duration maxExpectedInterleave) {
        this(streams, null, maxExpectedInterleave);
    }

    public DummyWorkloadFactory(Iterator<WorkloadStreams> streams,
                                Iterator<Operation<?>> alternativeLastOperations,
                                Duration maxExpectedInterleave) {
        this.streams = streams;
        this.alternativeLastOperations = alternativeLastOperations;
        this.maxExpectedInterleave = maxExpectedInterleave;
    }

    @Override
    public Workload createWorkload() throws WorkloadException {
        WorkloadStreams workloadStreams;
        if (null == alternativeLastOperations) {
            workloadStreams = streams.next();
        } else {
            workloadStreams = streams.next();
            List<Operation<?>> asynchronousNonDependencyOperationsToReturn = Lists.newArrayList(workloadStreams.asynchronousStream().nonDependencyOperations());
            asynchronousNonDependencyOperationsToReturn.remove(asynchronousNonDependencyOperationsToReturn.size() - 1);
            asynchronousNonDependencyOperationsToReturn.add(alternativeLastOperations.next());
            workloadStreams.setAsynchronousStream(
                    workloadStreams.asynchronousStream().dependentOperationTypes(),
                    asynchronousNonDependencyOperationsToReturn.iterator(),
                    workloadStreams.asynchronousStream().nonDependencyOperations()
            );
        }
        return new DummyWorkload(workloadStreams, maxExpectedInterleave);
    }
}

package com.ldbc.driver.workloads.dummy;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.validation.ValidationException;
import com.ldbc.driver.validation.WorkloadFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DummyWorkloadFactory implements WorkloadFactory {
    private final Iterator<WorkloadStreams> operations;
    private final Iterator<Operation<?>> alternativeLastOperations;
    private final Duration maxExpectedInterleave;

    public DummyWorkloadFactory(Iterator<WorkloadStreams> operations,
                                Duration maxExpectedInterleave) {
        this(operations, null, maxExpectedInterleave);
    }

    public DummyWorkloadFactory(Iterator<WorkloadStreams> operations,
                                Iterator<Operation<?>> alternativeLastOperations,
                                Duration maxExpectedInterleave) {
        this.operations = operations;
        this.alternativeLastOperations = alternativeLastOperations;
        this.maxExpectedInterleave = maxExpectedInterleave;
    }

    @Override
    public Workload createWorkload() throws WorkloadException{
        Iterator<Operation<?>> workloadOperations;
        if (null == alternativeLastOperations) {
            workloadOperations = operations.next();
        } else {
            List<Operation<?>> operationsToReturn = Lists.newArrayList(operations.next());
            operationsToReturn.remove(operationsToReturn.size() - 1);
            operationsToReturn.add(alternativeLastOperations.next());
            workloadOperations = operationsToReturn.iterator();
        }
        return new DummyWorkload(workloadOperations, maxExpectedInterleave);
    }
}
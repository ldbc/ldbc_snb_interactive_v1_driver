package com.ldbc.driver.workloads.dummy;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.Workload;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.validation.ValidationException;
import com.ldbc.driver.validation.WorkloadFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DummyWorkloadFactory implements WorkloadFactory {
    private final Iterator<Iterator<Operation<?>>> operations;
    private final Iterator<Operation<?>> alternativeLastOperations;
    private final Map<Class<? extends Operation>, OperationClassification> operationClassifications;
    private final Duration maxExpectedInterleave;

    public DummyWorkloadFactory(Iterator<Iterator<Operation<?>>> operations,
                                Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                                Duration maxExpectedInterleave) {
        this(operations, null, operationClassifications, maxExpectedInterleave);
    }

    public DummyWorkloadFactory(Iterator<Iterator<Operation<?>>> operations,
                                Iterator<Operation<?>> alternativeLastOperations,
                                Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                                Duration maxExpectedInterleave) {
        this.operations = operations;
        this.alternativeLastOperations = alternativeLastOperations;
        this.operationClassifications = operationClassifications;
        this.maxExpectedInterleave = maxExpectedInterleave;
    }

    @Override
    public Workload createWorkload() throws ValidationException {
        Iterator<Operation<?>> workloadOperations;
        if (null == alternativeLastOperations) {
            workloadOperations = operations.next();
        } else {
            List<Operation<?>> operationsToReturn = Lists.newArrayList(operations.next());
            operationsToReturn.remove(operationsToReturn.size() - 1);
            operationsToReturn.add(alternativeLastOperations.next());
            workloadOperations = operationsToReturn.iterator();
        }
        return new DummyWorkload(workloadOperations, operationClassifications, maxExpectedInterleave);
    }
}

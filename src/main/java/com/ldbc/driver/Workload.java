package com.ldbc.driver;

import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class Workload {
    public static Class<Operation<?>>[] operationTypesBySchedulingMode(Map<Class<? extends Operation<?>>, OperationClassification> operationClassificationMapping,
                                                                       OperationClassification.SchedulingMode schedulingMode) {
        List<Class<? extends Operation<?>>> operationsBySchedulingMode = new ArrayList<Class<? extends Operation<?>>>();
        for (Map.Entry<Class<? extends Operation<?>>, OperationClassification> operationAndClassification : operationClassificationMapping.entrySet()) {
            if (operationAndClassification.getValue().schedulingMode().equals(schedulingMode))
                operationsBySchedulingMode.add(operationAndClassification.getKey());
        }
        return operationsBySchedulingMode.toArray(new Class[operationsBySchedulingMode.size()]);
    }

    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    private long operationCount;

    public abstract Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications();

    /**
     * Called once to initialize state for workload
     */
    public final void init(DriverConfiguration params) throws WorkloadException {
        if (isInitialized) {
            throw new WorkloadException("Workload may be initialized only once");
        }
        isInitialized = true;
        this.operationCount = params.operationCount();
        onInit(params.asMap());
    }

    protected long getOperationCount() {
        return operationCount;
    }

    public abstract void onInit(Map<String, String> properties) throws WorkloadException;

    public final void cleanup() throws WorkloadException {
        if (isCleanedUp) {
            throw new WorkloadException("Workload may be cleaned up only once");
        }
        isCleanedUp = true;
        onCleanup();
    }

    protected abstract void onCleanup() throws WorkloadException;

    public final Iterator<Operation<?>> operations(GeneratorFactory generators)
            throws WorkloadException {
        if (ConsoleAndFileDriverConfiguration.UNBOUNDED_OPERATION_COUNT == getOperationCount()) {
            // Generate all workload operations before beginning
            return createOperations(generators);
        } else {
            // Generate all workload operations before beginning
            return generators.limit(createOperations(generators), getOperationCount());
        }
    }

    protected abstract Iterator<Operation<?>> createOperations(GeneratorFactory generators)
            throws WorkloadException;
}

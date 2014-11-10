package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.util.Tuple;

import java.util.*;

public class DbValidationResult {
    private final Db db;
    private final Set<Class> missingHandlersForOperationTypes;
    private final List<Tuple.Tuple2<Operation<?>, String>> unableToExecuteOperations;
    private final List<Tuple.Tuple3<Operation<?>, Object, Object>> incorrectResultsForOperations;
    private final Map<Class, Integer> successfullyExecutedOperationsPerOperationType;
    private final Map<Class, Integer> totalOperationsPerOperationType;

    DbValidationResult(Db db) {
        this.db = db;
        this.missingHandlersForOperationTypes = new HashSet<>();
        this.unableToExecuteOperations = new ArrayList<>();
        this.incorrectResultsForOperations = new ArrayList<>();
        this.successfullyExecutedOperationsPerOperationType = new HashMap<>();
        this.totalOperationsPerOperationType = new HashMap<>();
    }

    void reportMissingHandlerForOperation(Operation<?> operation) {
        missingHandlersForOperationTypes.add(operation.getClass());
        incrementOperationCountPerOperationType(operation.getClass());
    }

    void reportUnableToExecuteOperation(Operation<?> operation, String errorMessage) {
        unableToExecuteOperations.add(Tuple.<Operation<?>, String>tuple2(operation, errorMessage));
        incrementOperationCountPerOperationType(operation.getClass());
    }

    void reportIncorrectResultForOperation(Operation<?> operation, Object expectedResult, Object actualResult) {
        incorrectResultsForOperations.add(Tuple.<Operation<?>, Object, Object>tuple3(operation, expectedResult, actualResult));
        incrementOperationCountPerOperationType(operation.getClass());
    }

    void reportSuccessfulExecution(Operation<?> operation) {
        if (false == successfullyExecutedOperationsPerOperationType.containsKey(operation.getClass()))
            successfullyExecutedOperationsPerOperationType.put(operation.getClass(), 0);
        int successfullyExecutedOperationsForOperationType = successfullyExecutedOperationsPerOperationType.get(operation.getClass());
        successfullyExecutedOperationsForOperationType++;
        successfullyExecutedOperationsPerOperationType.put(operation.getClass(), successfullyExecutedOperationsForOperationType);
        incrementOperationCountPerOperationType(operation.getClass());
    }

    private void incrementOperationCountPerOperationType(Class operationType) {
        Integer count = totalOperationsPerOperationType.get(operationType);
        if (null == count) {
            totalOperationsPerOperationType.put(operationType, 1);
        } else {
            totalOperationsPerOperationType.put(operationType, count + 1);
        }
    }

    public boolean isSuccessful() {
        return missingHandlersForOperationTypes.isEmpty() && unableToExecuteOperations.isEmpty() && incorrectResultsForOperations.isEmpty();
    }

    public String actualResultsForFailedOperationsAsJsonString(Workload workload) throws WorkloadException {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < incorrectResultsForOperations.size() - 1; i++) {
            Operation<?> operation = incorrectResultsForOperations.get(i)._1();
            Object actualResult = incorrectResultsForOperations.get(i)._3();
            sb.append(operationAndResultAsJsonMapString(operation, actualResult, workload)).append(",");
        }
        if (incorrectResultsForOperations.size() >= 1) {
            Operation<?> operation = incorrectResultsForOperations.get(incorrectResultsForOperations.size() - 1)._1();
            Object actualResult = incorrectResultsForOperations.get(incorrectResultsForOperations.size() - 1)._3();
            sb.append(operationAndResultAsJsonMapString(operation, actualResult, workload));
        }
        sb.append("]");
        return sb.toString();
    }

    public String expectedResultsForFailedOperationsAsJsonString(Workload workload) throws WorkloadException {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < incorrectResultsForOperations.size() - 1; i++) {
            Operation<?> operation = incorrectResultsForOperations.get(i)._1();
            Object expectedResult = incorrectResultsForOperations.get(i)._2();
            sb.append(operationAndResultAsJsonMapString(operation, expectedResult, workload)).append(",");
        }
        if (incorrectResultsForOperations.size() >= 1) {
            Operation<?> operation = incorrectResultsForOperations.get(incorrectResultsForOperations.size() - 1)._1();
            Object expectedResult = incorrectResultsForOperations.get(incorrectResultsForOperations.size() - 1)._2();
            sb.append(operationAndResultAsJsonMapString(operation, expectedResult, workload));
        }
        sb.append("]");
        return sb.toString();
    }

    private String operationAndResultAsJsonMapString(Operation<?> operation, Object result, Workload workload) throws WorkloadException {
        String serializedOperation;
        try {
            serializedOperation = workload.serializeOperation(operation);
        } catch (SerializingMarshallingException e) {
            throw new WorkloadException(
                    String.format("Error occurred while serializing operation\nOperation: %s", operation),
                    e
            );
        }
        String serializedResult;
        try {
            serializedResult = operation.serializeResult(result);
        } catch (SerializingMarshallingException e) {
            throw new WorkloadException(
                    String.format("Error occurred while serializing operation result\nResult: %s", result.toString()),
                    e
            );
        }
        return "{\"operation\":" + serializedOperation + ",\"result\":" + serializedResult + "}";
    }

    public String resultMessage() {
        int padRightDistance = 10;
        StringBuilder sb = new StringBuilder();
        sb.append("Validation Result: ").append((isSuccessful()) ? "PASS" : "FAIL").append("\n");
        sb.append("Database: ").append(db.getClass().getName()).append("\n");
        sb.append("  ***\n");
        sb.append("  Successfully executed ").append(successfullyExecutedOperationsPerOperationType.size()).append(" operation types:\n");
        for (Class operationType : sort(totalOperationsPerOperationType.keySet()))
            sb.append("    ").
                    append((successfullyExecutedOperationsPerOperationType.containsKey(operationType)) ? successfullyExecutedOperationsPerOperationType.get(operationType) : 0).append(" / ").
                    append(String.format("%1$-" + padRightDistance + "s", totalOperationsPerOperationType.get(operationType))).
                    append(operationType.getSimpleName()).
                    append("\n");
        sb.append("  ***\n");
        sb.append("  Missing handler implementations for ").append(missingHandlersForOperationTypes.size()).append(" operation types:\n");
        for (Class operationType : sort(missingHandlersForOperationTypes))
            sb.append("    ").append(String.format("%1$-" + padRightDistance + "s", operationType.getName())).append("\n");
        sb.append("  ***\n");
        sb.append("  Unable to execute ").append(unableToExecuteOperations.size()).append(" operations:\n");
        for (Tuple.Tuple2<Operation<?>, String> failedOperationExecution : unableToExecuteOperations)
            sb.
                    append("    Operation: ").append(failedOperationExecution._1().toString()).append("\n").
                    append("               ").append(failedOperationExecution._2()).append("\n");
        sb.append("  ***\n");
        sb.append("  Incorrect results for ").append(incorrectResultsForOperations.size()).append(" operations:\n");
        for (Tuple.Tuple3<Operation<?>, Object, Object> incorrectResult : incorrectResultsForOperations)
            sb.
                    append("    Operation:        ").append(incorrectResult._1().toString()).append("\n").
                    append("    Expected Result:  ").append(incorrectResult._2().toString()).append("\n").
                    append("    Actual Result  :  ").append(incorrectResult._3().toString()).append("\n");
        sb.append("  ***\n");
        return sb.toString();
    }

    private <T> List<T> sort(Iterable<T> unsorted) {
        List<T> sorted = Lists.newArrayList(unsorted);
        Collections.sort(sorted, new DefaultComparator<T>());
        return sorted;
    }

    private static class DefaultComparator<T> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            if (o1 instanceof Comparable)
                return ((Comparable) o1).compareTo(o2);
            else
                return o1.toString().compareTo(o2.toString());
        }
    }
}

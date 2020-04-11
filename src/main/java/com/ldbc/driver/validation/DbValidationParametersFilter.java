package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;

import java.util.ArrayList;
import java.util.List;

public abstract class DbValidationParametersFilter {

    private final List<Operation> injectedOperations = new ArrayList<>();
    int validationParameterCount = 0;
    int requiredValidationParameterCount;

    public DbValidationParametersFilter() {
    }

    public DbValidationParametersFilter(final Integer requiredValidationParameterCount) {
        this.requiredValidationParameterCount = requiredValidationParameterCount;
    }

    public boolean useOperation(Operation operation) {
        return true;
    }

    public DbValidationParametersFilterResult useOperationAndResultForValidation(
            Operation operation,
            Object operationResult) {
        if (validationParameterCount < requiredValidationParameterCount) {
            validationParameterCount++;
            return new DbValidationParametersFilterResult(
                    DbValidationParametersFilterAcceptanceType.ACCEPT_AND_CONTINUE,
                    injectedOperations
            );
        } else {
            return new DbValidationParametersFilterResult(
                    DbValidationParametersFilterAcceptanceType.REJECT_AND_FINISH,
                    injectedOperations
            );
        }
    }
}



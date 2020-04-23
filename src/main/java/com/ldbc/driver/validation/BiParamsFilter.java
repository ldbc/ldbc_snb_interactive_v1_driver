package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;

import java.util.ArrayList;
import java.util.List;

import static com.ldbc.driver.validation.FilterAcceptanceType.*;

public class BiParamsFilter implements ParamsFilter {

    private final List<Operation> injectedOperations = new ArrayList<>();
    int validationParameterCount = 0;
    int requiredValidationParameterCount;

    public BiParamsFilter(final Integer requiredValidationParameterCount) {
        this.requiredValidationParameterCount = requiredValidationParameterCount;
    }

    @Override
    public boolean useOp(Operation operation) {
        return true;
    }

    public FilterResult useOpAndRes(
            Operation operation,
            Object operationResult) {
        if (validationParameterCount < requiredValidationParameterCount) {
            validationParameterCount++;
            return new FilterResult(
                    ACCEPT_AND_CONTINUE,
                    injectedOperations
            );
        } else {
            return new FilterResult(
                    REJECT_AND_FINISH,
                    injectedOperations
            );
        }
    }

}

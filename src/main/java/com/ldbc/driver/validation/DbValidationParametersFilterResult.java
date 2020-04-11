package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.validation.DbValidationParametersFilterAcceptanceType;

import java.util.List;

public class DbValidationParametersFilterResult {

    private final DbValidationParametersFilterAcceptanceType acceptanceType;
    private final List<Operation> injectedOperations;

    public DbValidationParametersFilterResult(
            DbValidationParametersFilterAcceptanceType acceptance,
            List<Operation> injectedOperations) {
        this.acceptanceType = acceptance;
        this.injectedOperations = injectedOperations;
    }

    public DbValidationParametersFilterAcceptanceType acceptance() {
        return acceptanceType;
    }

    public List<Operation> injectedOperations() {
        return injectedOperations;
    }
}

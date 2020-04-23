package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;

import java.util.List;

public class FilterResult {

    private final FilterAcceptanceType acceptanceType;
    private final List<Operation> injectedOperations;

    public FilterResult(FilterAcceptanceType acceptance, List<Operation> injectedOperations) {
        this.acceptanceType = acceptance;
        this.injectedOperations = injectedOperations;
    }

    public FilterAcceptanceType acceptance() {
        return acceptanceType;
    }

    public List<Operation> injectedOperations() {
        return injectedOperations;
    }
}

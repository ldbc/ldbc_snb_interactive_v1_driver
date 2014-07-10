package com.ldbc.driver.validation;

public class WorkloadValidationResult {
    private final boolean successful;
    private final String errorMessage;

    public WorkloadValidationResult(boolean successful, String errorMessage) {
        this.successful = successful;
        this.errorMessage = errorMessage;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public String errorMessage() {
        return errorMessage;
    }
}

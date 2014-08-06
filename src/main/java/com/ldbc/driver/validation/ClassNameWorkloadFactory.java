package com.ldbc.driver.validation;

import com.ldbc.driver.Workload;
import com.ldbc.driver.util.ClassLoaderHelper;

public class ClassNameWorkloadFactory implements WorkloadFactory {
    private final String workloadClassName;

    public ClassNameWorkloadFactory(String workloadClassName) {
        this.workloadClassName = workloadClassName;
    }

    public Workload createWorkload() throws ValidationException {
        try {
            return ClassLoaderHelper.loadWorkload(workloadClassName);
        } catch (Exception e) {
            throw new ValidationException(String.format("Error loading Workload class: %s", workloadClassName), e);
        }
    }
}

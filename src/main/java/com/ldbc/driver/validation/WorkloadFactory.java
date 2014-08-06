package com.ldbc.driver.validation;

import com.ldbc.driver.Workload;

public interface WorkloadFactory {
    Workload createWorkload() throws ValidationException;
}

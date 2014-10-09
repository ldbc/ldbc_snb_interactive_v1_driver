package com.ldbc.driver.validation;

import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;

public interface WorkloadFactory {
    Workload createWorkload() throws WorkloadException;
}

package com.ldbc.driver.workloads;

import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;

public interface WorkloadFactory
{
    Workload createWorkload() throws WorkloadException;
}

package org.ldbcouncil.driver.workloads;

import org.ldbcouncil.driver.Workload;
import org.ldbcouncil.driver.WorkloadException;

public interface WorkloadFactory
{
    Workload createWorkload() throws WorkloadException;
}

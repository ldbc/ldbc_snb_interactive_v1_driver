package org.ldbcouncil.snb.driver.workloads;

import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.WorkloadException;

public interface WorkloadFactory
{
    Workload createWorkload() throws WorkloadException;
}

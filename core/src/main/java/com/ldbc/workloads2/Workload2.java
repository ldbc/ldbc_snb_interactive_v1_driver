package com.ldbc.workloads2;

import java.util.Map;

import com.ldbc.db2.Operation2;
import com.ldbc.db2.OperationGenerator2;
import com.ldbc.generator.GeneratorBuilder;

public abstract class Workload2
{
    public Workload2( Map<String, String> properties, GeneratorBuilder generatorBuilder )
    {
    }

    public abstract OperationGenerator2<Operation2> getLoadOperations() throws WorkloadException2;

    public abstract OperationGenerator2<Operation2> getTransactionalOperations() throws WorkloadException2;
}

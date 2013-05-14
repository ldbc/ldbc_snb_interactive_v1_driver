package com.ldbc.workloads2;

import java.util.Map;
import com.ldbc.generator.GeneratorBuilder;

public abstract class Workload2
{
    public Workload2( Map<String, String> properties, GeneratorBuilder generatorBuilder )
    {
    }

    public abstract OperationGenerator2<OperationHandler2> getLoadOperations() throws WorkloadException2;

    public abstract OperationGenerator2<OperationHandler2> getTransactionalOperations() throws WorkloadException2;
}

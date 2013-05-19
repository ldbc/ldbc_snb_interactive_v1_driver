package com.ldbc.workloads2;

import java.util.Map;

import com.ldbc.db2.Operation2;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;

public abstract class Workload2
{
    public Workload2( Map<String, String> properties ) throws WorkloadException2
    {
    }

    public abstract Generator<Operation2<?>> getLoadOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException2;

    public abstract Generator<Operation2<?>> getTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException2;
}

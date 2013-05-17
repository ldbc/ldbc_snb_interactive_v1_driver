package com.ldbc.workloads2;

import java.util.Map;

import com.ldbc.db2.Operation2;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;

public abstract class Workload2
{
    /* 
     * INSERT_START
     * Specifies which record ID each client starts from - enables load phase to proceed from 
     * multiple clients on different machines.
     * 
     * INSERT_COUNT
     * Interpreted by Client, tells each client instance of the how many inserts to do.
     *  
     * E.g. to load 1,000,000 records from 2 machines: 
     * client 1 --> insertStart=0
     *          --> insertCount=500,000
     * client 2 --> insertStart=50,000
     *          --> insertCount=500,000
    */
    public static final String INSERT_START = "insertstart";
    public static final String INSERT_START_DEFAULT = "0";

    public Workload2( Map<String, String> properties ) throws WorkloadException2
    {
    }

    public abstract Generator<Operation2<?>> getLoadOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException2;

    public abstract Generator<Operation2<?>> getTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException2;
}

package com.ldbc.workloads2;

public class WorkloadException2 extends Exception
{
    private static final long serialVersionUID = 8844396756042772132L;

    public WorkloadException2( String message )
    {
        super( message );
    }

    public WorkloadException2()
    {
        super();
    }

    public WorkloadException2( String message, Throwable cause )
    {
        super( message, cause );
    }

    public WorkloadException2( Throwable cause )
    {
        super( cause );
    }
}

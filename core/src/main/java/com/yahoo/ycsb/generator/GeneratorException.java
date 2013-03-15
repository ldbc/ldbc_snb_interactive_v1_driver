package com.yahoo.ycsb.generator;

public class GeneratorException extends Exception
{
    private static final long serialVersionUID = -5243628117143814942L;

    public GeneratorException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public GeneratorException( String message )
    {
        super( message );
    }

    public GeneratorException( Throwable cause )
    {
        super( cause );
    }
}

package com.ldbc.driver.util.temporal;

public interface MultipleTimeUnitProvider<T>
{
    public long asSeconds();

    public long asMilli();

    public long asMicro();

    public long asNano();

    public long as( TimeUnit timeUnit );

    public boolean greatThan( T other );

    public boolean lessThan( T other );

    public Duration greaterBy( T other );

    public Duration lessBy( T other );

    public T plus( Duration duration );

    public T minus( Duration duration );
}

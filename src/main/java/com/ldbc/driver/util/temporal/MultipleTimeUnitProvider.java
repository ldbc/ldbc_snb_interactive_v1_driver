package com.ldbc.driver.util.temporal;

import java.util.concurrent.TimeUnit;

public interface MultipleTimeUnitProvider<T>
{
    public Long asSeconds();

    public Long asMilli();

    public Long asMicro();

    public Long asNano();

    public Long as( TimeUnit timeUnit );

    public boolean greatThan( T other );

    public boolean lessThan( T other );

    public Duration greaterBy( T other );

    public Duration lessBy( T other );

    public T plus( Duration duration );

    public T minus( Duration duration );
}
